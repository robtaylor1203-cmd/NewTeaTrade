import sqlite3
# Import PlaywrightTimeoutError specifically for robust error handling
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timezone
from urllib.parse import urljoin
import time
import random
import os
from bs4 import BeautifulSoup, Comment

# We need to ensure fuzzywuzzy is installed (pip install fuzzywuzzy python-Levenshtein)
try:
    from fuzzywuzzy import fuzz
except ImportError:
    print("Warning: fuzzywuzzy not found. Deduplication will be less effective.")
    # Define a fallback if fuzzywuzzy is missing
    class fuzz:
        @staticmethod
        def ratio(s1, s2):
            return 100 if s1 == s2 else 0

DB_FILE = "news.db"
HTML_FILE = "news.html"
MAX_PAGES_PER_SOURCE = 3 # Limit pagination depth

# Configuration for stability
SELECTOR_TIMEOUT = 30000 # 30 seconds for content to appear
NAVIGATION_TIMEOUT = 60000 # 60 seconds for navigation
# 'domcontentloaded' is generally faster and more reliable for scraping initialization
NAVIGATION_WAIT_STRATEGY = "domcontentloaded" 

def initialize_database():
    """Creates the news table in the database if it doesn't exist."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY,
                    headline TEXT NOT NULL,
                    snippet TEXT,
                    source TEXT NOT NULL,
                    link TEXT NOT NULL UNIQUE,
                    scraped_date TEXT NOT NULL,
                    article_date TEXT
                )
            """)
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")

def article_exists(headline, link, conn):
    """Checks if an article already exists to prevent duplicates."""
    try:
        cursor = conn.cursor()
        # Check by link first (fastest)
        cursor.execute("SELECT 1 FROM articles WHERE link = ?", (link,))
        if cursor.fetchone():
            return True
        
        # Check by headline similarity (slower)
        cursor.execute("SELECT headline FROM articles")
        for row in cursor.fetchall():
            if fuzz.ratio(headline, row[0]) > 90:
                return True
        return False
    except sqlite3.Error as e:
        print(f"Database check error: {e}")
        return False

# =============================================================================
# DEBUGGING & HELPERS
# =============================================================================

def save_debug_files(page, prefix="debug"):
    """Saves a screenshot and the HTML source of the current page upon failure."""
    try:
        # Ensure the prefix is safe for filenames
        safe_prefix = "".join([c for c in prefix if c.isalpha() or c.isdigit() or c=='_']).rstrip()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"{safe_prefix}_{timestamp}_screenshot.png"
        html_path = f"{safe_prefix}_{timestamp}_source.html"
        
        page.screenshot(path=screenshot_path, full_page=True)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"  [DEBUG] Timeout occurred. Saved debug files: {screenshot_path} and {html_path}")
    except Exception as e:
        print(f"  [DEBUG] Could not save debug files: {e}")

def handle_consent(page, source_name):
    """
    Handles specific cookie consent banners by explicitly waiting for and clicking them.
    """
    # Configuration defines how to handle consent per source
    consent_config = {
        "BBC News": {
            "selectors": [
                'button[aria-label*="agree" i]',
                'button:has-text("Yes, I agree")',
                'button[data-testid="banner-accept"]',
                'p[data-bbc-content-id="bbccookies-continue-button"]',
                'button:has-text("Allow all")',
                'button:has-text("Accept recommended cookies")'
            ],
            "wait_after_click": "domcontentloaded" # BBC often reloads/redirects
        },
        "Euronews": {
            "selectors": [
                '#didomi-notice-agree-button',
                'button:has-text("Agree and close")',
                'button:has-text("AGREE")'
            ],
            "wait_after_click": "hidden" # Euronews usually just hides the banner
        },
    }

    config = consent_config.get(source_name)
    if not config:
        return False # No specific handling defined

    combined_selector = ", ".join(config["selectors"])

    try:
        # 1. Wait explicitly for the banner to appear (up to 10 seconds)
        print(f"  [Consent] Waiting up to 10s for {source_name} consent banner...")
        page.wait_for_selector(combined_selector, state='visible', timeout=10000)
        
        # 2. Click the first visible button (use force=True in case of overlays)
        button = page.locator(combined_selector).first
        if button.is_visible():
            print(f"  [Consent] Found banner. Clicking consent button.")
            # Use force=True to click even if slightly obscured
            button.click(timeout=5000, force=True)
            
            # 3. Wait for the UI to update
            if config["wait_after_click"] == "domcontentloaded":
                try:
                    # Wait for navigation/reload if expected
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                except PlaywrightTimeoutError:
                    time.sleep(1) # Fallback if navigation doesn't occur
            elif config["wait_after_click"] == "hidden":
                # Wait for the button/banner to disappear
                button.wait_for(state="hidden", timeout=10000)
            
            return True
    except PlaywrightTimeoutError:
         print(f"  [Consent] No {source_name} consent banner found within 10s or failed to hide.")
         return False
    except Exception as e:
        print(f"  [Consent] Error during {source_name} consent handling: {e}")
        return False
    return False

# =============================================================================
# SCRAPER FUNCTIONS
# =============================================================================

def scrape_tea_and_coffee_news(page):
    """Scrapes articles from teaandcoffee.net/news with pagination and ad filtering."""
    source_name = "Tea & Coffee Trade Journal"
    base_url = "https://www.teaandcoffee.net/news/"
    print(f"Scraping {source_name}...")
    
    articles = []
    current_page_num = 1

    while current_page_num <= MAX_PAGES_PER_SOURCE:
        # Construct the URL for the current page (URL-based pagination)
        url = base_url if current_page_num == 1 else f"{base_url}page/{current_page_num}/"
        print(f"  Navigating to page {current_page_num}: {url}")
        
        try:
            response = page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
            
            # If the page returns a 404 or similar error (and it's not the first page), stop pagination
            if response and response.status >= 404 and current_page_num > 1:
                 print(f"  Page {current_page_num} not found (Status: {response.status}). Stopping pagination.")
                 break

            # Wait for the specific container holding the articles (more precise than main#main)
            page.wait_for_selector('div.articles.block.category', state='visible', timeout=SELECTOR_TIMEOUT)
        
        except Exception as e:
            print(f"  Error navigating to or loading content at {url}: {e}")
            if isinstance(e, PlaywrightTimeoutError):
                save_debug_files(page, f"debug_TC_Page{current_page_num}")
            break # Stop if navigation fails

        # Target specific articles within the container
        # The structure is: div.articles.block.category > article
        article_locators = page.locator('div.articles.block.category > article')

        if article_locators.count() == 0:
            print("  No recognizable articles found on this page. Stopping pagination.")
            break

        for item in article_locators.all():
            try:
                # [MODIFIED] - Improved ad/sponsored content filtering and structure check
                # Check for explicit ad markers in text or if the item lacks a proper headline link.
                if item.locator("text=/sponsored|advertisement|AD\s*\|/i").count() > 0 or item.locator('h3 a').count() == 0:
                    continue

                # Robust extraction of elements
                headline_element = item.locator('h3 a').first
                headline = headline_element.inner_text(timeout=5000)
                link = headline_element.get_attribute('href', timeout=5000)
                
                # Look for the excerpt specifically
                snippet_element = item.locator('div.articleExcerpt')
                snippet = snippet_element.inner_text(timeout=5000) if snippet_element.count() > 0 else ""
                
                date_element = item.locator('div.meta')
                article_date = date_element.inner_text(timeout=5000) if date_element.count() > 0 else ""

                # Basic filtering: ensure headline and link are present
                if headline and link:
                    # Ensure link is absolute
                    full_link = urljoin(base_url, link)
                    
                    articles.append({
                        "headline": headline.strip(),
                        "snippet": snippet.strip(),
                        "source": source_name,
                        "link": full_link,
                        "article_date": article_date.strip()
                    })
            except Exception as e:
                print(f"  Could not process an item on {source_name}: {e}")

        current_page_num += 1
        time.sleep(random.uniform(1, 3)) # Pause slightly between pages

    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_bbc_news(page):
    """Scrapes articles from the BBC News Tea topic page."""
    source_name = "BBC News"
    url = "https://www.bbc.co.uk/news/topics/c50nyrxjl4lt"
    print(f"Scraping {source_name}...")
    
    articles = []
    
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        
        # Handle Cookie Consent Banners (Critical for BBC)
        handle_consent(page, source_name)

        # Wait for the main container first.
        page.wait_for_selector('main#main-content', state='visible', timeout=SELECTOR_TIMEOUT)

    except Exception as e:
        print(f"  Error navigating, handling consent, or loading main container at {url}: {e}")
        if isinstance(e, PlaywrightTimeoutError):
            save_debug_files(page, "debug_BBC")
        return articles

    # Define the article selectors
    content_selector = 'div[data-testid="topic-card"], div[data-testid^="promo-"]'
    
    # Gracefully handle empty topic pages. Wait briefly (e.g., 5s) to see if articles load via JS.
    try:
        page.wait_for_selector(content_selector, state='visible', timeout=5000)
    except PlaywrightTimeoutError:
        # If timeout occurs after 5s, the topic page is likely empty.
        print("  No articles found on BBC Tea topic page after waiting 5s. The topic page might be empty.")
        return articles # Exit gracefully

    # Proceed with extraction if articles exist
    article_locators = page.locator(content_selector)
    for item in article_locators.all():
        try:
            # Headline and Link extraction
            link_element = item.locator('a[data-testid="internal-link"], h2 a, h3 a').first

            # [MODIFIED] - Check if link element exists and is visible
            if link_element.count() == 0 or not link_element.is_visible(timeout=5000):
                continue

            headline = link_element.inner_text(timeout=5000)
            link = link_element.get_attribute('href', timeout=5000)

            # Snippet extraction
            snippet_element = item.locator('p[data-testid="card-description"], p[data-testid="promo-summary"], p').first
            snippet = snippet_element.inner_text(timeout=5000) if snippet_element.count() > 0 and snippet_element.is_visible(timeout=5000) else ""

            # Date extraction
            date_element = item.locator('time[data-testid="card-metadata-lastupdated"], time[data-testid="timestamp"], time').first
            article_date = date_element.inner_text(timeout=5000) if date_element.count() > 0 and date_element.is_visible(timeout=5000) else ""
            
            # Basic filtering
            if headline and link:
                # Ensure link is absolute
                full_link = urljoin("https://www.bbc.co.uk", link)
                
                # Filter out potential non-content links
                if "/news/" not in full_link and "/sport/" not in full_link:
                    continue
                    
                articles.append({
                    "headline": headline.strip(),
                    "snippet": snippet.strip(),
                    "source": source_name,
                    "link": full_link,
                    "article_date": article_date.strip()
                })
        except Exception as e:
            print(f"  Could not process an item on {source_name}: {e}")

    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_euronews(page):
    """Scrapes articles from the Euronews Tea tag page."""
    source_name = "Euronews"
    url = "https://www.euronews.com/tag/tea"
    print(f"Scraping {source_name}...")
    
    articles = []
    
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)

        # Handle Cookie Consent (Euronews often uses 'didomi')
        handle_consent(page, source_name)

        # Wait for articles to load. Changed state to 'attached' instead of 'visible'
        # as optimization CSS might hide elements even when they are in the DOM.
        page.wait_for_selector('article.m-object', state='attached', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  Error navigating to or loading {url}: {e}")
        if isinstance(e, PlaywrightTimeoutError):
            save_debug_files(page, "debug_Euronews")
        return articles

    # Euronews structure: Exclude articles containing advertising divs.
    for item in page.locator('article.m-object:not(:has(div.c-advertising))').all():
        try:
            # Explicit check for "Sponsored" labels
            if item.locator('span:has-text("Sponsored"), span:has-text("Advertisement")').count() > 0:
                continue

            # Headline and Link
            headline_element = item.locator('a.m-object__title__link')
            # [MODIFIED] - Ensure the headline element exists before proceeding
            if headline_element.count() == 0: continue

            # Use the 'title' attribute if present, otherwise the inner text
            headline = headline_element.first.get_attribute('title', timeout=5000) or headline_element.first.inner_text(timeout=5000)
            link = headline_element.first.get_attribute('href', timeout=5000)
            
            # Snippet (Euronews often doesn't show snippets on tag pages)
            snippet = "" 

            # Date (Difficult to extract reliably on this view)
            article_date = ""

            # Basic filtering
            if headline and link:
                # Ensure link is absolute
                full_link = urljoin("https://www.euronews.com", link)
                
                articles.append({
                    "headline": headline.strip(),
                    "snippet": snippet.strip(),
                    "source": source_name,
                    "link": full_link,
                    "article_date": article_date.strip()
                })
        except Exception as e:
            print(f"  Could not process an item on {source_name}: {e}")

    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

# =============================================================================
# HTML INJECTION (No significant changes needed here)
# =============================================================================

def inject_html(articles):
    """Injects the scraped articles into the HTML file."""
    print("Injecting articles into HTML...")
    
    try:
        with open(HTML_FILE, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
    except FileNotFoundError:
        print(f"Error: {HTML_FILE} not found. Cannot inject articles.")
        return
    
    # Find the injection markers
    start_tag = soup.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
    end_tag = soup.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)
    
    if not start_tag or not end_tag:
        print("Could not find the and tags in the HTML file.")
        return
        
    # Clear existing content between the tags robustly
    current = start_tag.next_sibling
    while current and current != end_tag:
        next_tag = current.next_sibling
        if hasattr(current, 'decompose'):
            current.decompose()
        else:
            # Handle navigable strings (like whitespace)
            current.extract()
        current = next_tag
        
    # Generate HTML for the articles
    articles_html = ""
    for article in articles:
        # Handle potential None values for snippet
        snippet_text = article['snippet'] if article['snippet'] else ""
        
        # Format source and date display
        if article.get('article_date'):
            source_date_text = f"{article['source']} - <span class=\"article-date\">{article['article_date']}</span>"
        else:
            # Fallback: Use the scraped date if the article date is missing
            try:
                # Parse ISO format (handles UTC 'Z' or timezone offsets)
                scraped_date_str = article['scraped_date']
                scraped_dt = datetime.fromisoformat(scraped_date_str.replace('Z', '+00:00'))
                
                # Format for display
                formatted_date = scraped_dt.strftime("%d %b %Y")
                source_date_text = f"{article['source']} - <span class=\"article-date\">{formatted_date}</span>"
            except Exception as e:
                # Final fallback if parsing fails
                source_date_text = article['source']

        
        articles_html += f"""
            <article class="news-item">
                <div class="text-content">
                    <a href="{article['link']}" class="main-link" target="_blank" rel="noopener noreferrer">
                        <h3>{article['headline']}</h3>
                        <p class="snippet">{snippet_text}</p>
                    </a>
                    <div class="source">{source_date_text}</div>
                </div>
            </article>
        """
        
    # Insert the new HTML after the start tag
    start_tag.insert_after(BeautifulSoup(articles_html, "html.parser"))
    
    # Write the updated HTML back to the file
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(str(soup))
        
    print(f"Successfully injected {len(articles)} articles into {HTML_FILE}.")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function to run all scrapers, update the database, and rebuild the HTML."""
    start_time = time.time()
    initialize_database()

    all_scraped_articles = []
    
    # Define the list of scraper functions to run
    scrapers = [
        scrape_tea_and_coffee_news,
        scrape_bbc_news,
        scrape_euronews
    ]

    # Initialize Playwright
    try:
        with sync_playwright() as p:
            # Set headless=False for local debugging so you can watch the browser.
            # Set slow_mo=100 to slightly slow down actions, making it easier to follow.
            browser = p.chromium.launch(headless=False, slow_mo=100) 
            
            # Create a new context with a realistic User-Agent
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            # Run each scraper sequentially
            for scraper_func in scrapers:
                try:
                    scraped_data = scraper_func(page)
                    all_scraped_articles.extend(scraped_data)
                except Exception as e:
                    print(f"Error running scraper {scraper_func.__name__}: {e}")
                # Pause between sources
                time.sleep(random.uniform(2, 5)) 

            browser.close()
    except Exception as e:
        print(f"Playwright initialization or execution error: {e}")
        return

    if not all_scraped_articles:
        print("\nNo articles were successfully scraped in this run.")
    
    # Process and insert into database
    new_articles_count = 0
    # Get the current UTC time once for this batch
    # Ensure the timestamp includes timezone information (+00:00 for UTC)
    scraped_timestamp = datetime.now(timezone.utc).isoformat()

    try:
        with sqlite3.connect(DB_FILE) as conn:
            for article in all_scraped_articles:
                if not article_exists(article['headline'], article['link'], conn):
                    conn.execute("""
                        INSERT INTO articles (headline, snippet, source, link, scraped_date, article_date)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        article['headline'],
                        article.get('snippet'),
                        article['source'],
                        article['link'],
                        scraped_timestamp,
                        article.get('article_date')
                    ))
                    new_articles_count += 1
    except sqlite3.Error as e:
        print(f"Database insertion error: {e}")

    print(f"\nScraping complete. Added {new_articles_count} new articles to the database.")

    # Retrieve all articles for HTML injection
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Order primarily by scraped date (newest first)
            cursor.execute("""
                SELECT headline, snippet, source, link, article_date, scraped_date
                FROM articles
                ORDER BY
                    scraped_date DESC,
                    article_date DESC
                LIMIT 300
            """)
            all_db_articles = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database retrieval error: {e}")
        all_db_articles = []

    # Inject articles into the HTML
    if all_db_articles:
        inject_html(all_db_articles)
    else:
        print("No articles retrieved from the database for injection.")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()