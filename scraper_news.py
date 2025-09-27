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
    consent_config = {
        "BBC News": {
            "selectors": [
                'button:has-text("Accept additional cookies")',
                'button[aria-label*="agree" i]',
                'button:has-text("Yes, I agree")',
                'button[data-testid="banner-accept"]',
                'p[data-bbc-content-id="bbccookies-continue-button"]',
                'button:has-text("Allow all")',
            ],
            "wait_after_click": "domcontentloaded"
        },
        "Euronews": {
            "selectors": [
                '#didomi-notice-agree-button',
                'button:has-text("Agree and close")',
                'button:has-text("AGREE")'
            ],
            "wait_after_click": "hidden"
        },
        "Tea & Coffee Trade Journal": {
            "selectors": [
                'button:has-text("Accept")',
                'button:has-text("Allow all")',
                'button:has-text("Agree")'
            ],
             "wait_after_click": "hidden"
        }
    }

    config = consent_config.get(source_name)
    if not config:
        return False

    combined_selector = ", ".join(config["selectors"])

    try:
        print(f"  [Consent] Waiting up to 10s for {source_name} consent banner...")
        page.wait_for_selector(combined_selector, state='visible', timeout=10000)
        
        button = page.locator(combined_selector).first
        if button.is_visible():
            print(f"  [Consent] Found banner. Clicking consent button.")
            button.click(timeout=5000, force=True)
            
            if config["wait_after_click"] == "domcontentloaded":
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                except PlaywrightTimeoutError:
                    time.sleep(2)
            elif config["wait_after_click"] == "hidden":
                button.wait_for(state="hidden", timeout=10000)
                time.sleep(2)
            
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
    """ Scrapes teaandcoffee.net using click-based pagination."""
    source_name = "Tea & Coffee Trade Journal"
    url = "https://www.teaandcoffee.net/news/"
    print(f"Scraping {source_name}...")
    
    articles = []
    
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, source_name)
        page.wait_for_selector('div.flex.facetwp-template', state='visible', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  Error navigating to or loading initial page at {url}: {e}")
        if isinstance(e, PlaywrightTimeoutError):
            save_debug_files(page, "debug_TC_InitialLoad")
        return articles

    for page_num in range(1, MAX_PAGES_PER_SOURCE + 1):
        print(f"  Processing page {page_num}...")
        
        loading_spinner = page.locator('div.facetwp-loading')
        article_locators = page.locator('div.flex.facetwp-template > article.row3')
        
        for item in article_locators.all():
            try:
                if item.locator(r"text=/sponsored|advertisement|AD\s*\|/i").count() > 0 or item.locator('h3 a').count() == 0:
                    continue

                headline_element = item.locator('h3 a').first
                headline = headline_element.inner_text(timeout=5000)
                link = headline_element.get_attribute('href', timeout=5000)
                
                snippet_element = item.locator('div.articleExcerpt')
                snippet = snippet_element.inner_text(timeout=5000).strip() if snippet_element.count() > 0 else ""
                # [MODIFIED] Clean up the snippet text
                if snippet.upper().startswith("NEWS"):
                    snippet = snippet[4:].strip()

                date_element = item.locator('div.meta')
                article_date = date_element.inner_text(timeout=5000).strip() if date_element.count() > 0 else ""

                if headline and link:
                    full_link = urljoin(url, link)
                    articles.append({
                        "headline": headline.strip(), "snippet": snippet, "source": source_name,
                        "link": full_link, "article_date": article_date
                    })
            except Exception as e:
                print(f"  Could not process an item on {source_name}: {e}")

        if page_num == MAX_PAGES_PER_SOURCE:
            print("  Reached max page limit.")
            break

        next_button = page.locator('a.facetwp-page.next')
        if next_button.count() > 0 and next_button.is_visible():
            print("  Navigating to next page...")
            next_button.click()
            try:
                loading_spinner.wait_for(state="hidden", timeout=15000)
                time.sleep(random.uniform(1,2))
            except PlaywrightTimeoutError:
                print("  Pagination timed out waiting for content to load. Stopping.")
                break
        else:
            print("  No 'Next' button found. Stopping pagination.")
            break
            
    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_bbc_news(page):
    """Scrapes articles from BBC News with anti-bot measures."""
    source_name = "BBC News"
    url = "https://www.bbc.co.uk/news/topics/c50nyrxjl4lt"
    print(f"Scraping {source_name}...")
    
    articles = []
    
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, source_name)
        page.wait_for_load_state('networkidle', timeout=15000)
        page.wait_for_selector('main#main-content', state='visible', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  Error navigating, handling consent, or loading main container at {url}: {e}")
        if isinstance(e, PlaywrightTimeoutError):
            save_debug_files(page, "debug_BBC")
        return articles

    content_selector = 'div[class*="PromoCard"], div[data-testid^="promo-"]'
    
    try:
        page.wait_for_selector(content_selector, state='attached', timeout=5000)
    except PlaywrightTimeoutError:
        print("  No articles found on BBC Tea topic page after waiting 5s. The topic page might be empty.")
        return articles

    article_locators = page.locator(content_selector)
    for item in article_locators.all():
        try:
            link_element = item.locator('h2 a, h3 a').first
            if link_element.count() == 0 or not link_element.is_visible(timeout=5000):
                continue

            headline = link_element.inner_text(timeout=5000)
            link = link_element.get_attribute('href', timeout=5000)

            snippet_element = item.locator('p[data-testid="card-description"], p[class*="Paragraph"]').first
            snippet = snippet_element.inner_text(timeout=5000) if snippet_element.count() > 0 and snippet_element.is_visible(timeout=5000) else ""

            date_element = item.locator('time[data-testid*="timestamp"], time').first
            article_date = date_element.inner_text(timeout=5000) if date_element.count() > 0 and date_element.is_visible(timeout=5000) else ""
            
            if headline and link:
                full_link = urljoin("https://www.bbc.co.uk", link)
                if "/news/" not in full_link and "/sport/" not in full_link:
                    continue
                articles.append({
                    "headline": headline.strip(), "snippet": snippet.strip(), "source": source_name,
                    "link": full_link, "article_date": article_date.strip()
                })
        except Exception as e:
            print(f"  Could not process an item on {source_name}: {e}")

    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_euronews(page):
    """ Scrapes articles from the Euronews Tea tag page."""
    source_name = "Euronews"
    url = "https://www.euronews.com/tag/tea"
    print(f"Scraping {source_name}...")
    
    articles = []
    
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, source_name)
        page.wait_for_selector('section[data-block="listing"]', state='visible', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  Error navigating to or loading {url}: {e}")
        if isinstance(e, PlaywrightTimeoutError):
            save_debug_files(page, "debug_Euronews")
        return articles

    article_selector = 'article.the-media-object:not(.the-media-object--has-sponsored):not(:has-text("In partnership with"))'
    for item in page.locator(article_selector).all():
        try:
            headline_element = item.locator('h3.the-media-object__title')
            link_element = item.locator('a.the-media-object__link')
            
            if headline_element.count() == 0 or link_element.count() == 0:
                continue

            headline = headline_element.first.inner_text(timeout=5000)
            link = link_element.first.get_attribute('href', timeout=5000)
            
            snippet_element = item.locator('div.the-media-object__description')
            snippet = snippet_element.inner_text(timeout=5000) if snippet_element.count() > 0 else ""

            date_element = item.locator('div.the-media-object__date > time')
            article_date = date_element.get_attribute('datetime', timeout=5000) if date_element.count() > 0 else ""

            if headline and link:
                full_link = urljoin("https://www.euronews.com", link)
                articles.append({
                    "headline": headline.strip(), "snippet": snippet.strip(), "source": source_name,
                    "link": full_link, "article_date": article_date.strip()
                })
        except Exception as e:
            print(f"  Could not process an item on {source_name}: {e}")

    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

# =============================================================================
# HTML INJECTION
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
    
    # [MODIFIED] Target the correct container for injection
    injection_point = soup.find('div', id='news-container')
    if not injection_point:
        print(f"Error: Could not find <div id='news-container'> in {HTML_FILE}. Cannot inject articles.")
        return

    start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
    end_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)
    
    # Self-healing logic if tags are missing from the correct container
    if not start_tag or not end_tag:
        print(f"Warning: Injection markers not found inside #news-container. Rebuilding container.")
        injection_point.clear() 
        injection_point.append(Comment(" START_NEWS "))
        injection_point.append(Comment(" END_NEWS "))
        start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
        end_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)

    # Clear existing content between the tags
    current = start_tag.next_sibling
    while current and current != end_tag:
        next_tag = current.next_sibling
        if hasattr(current, 'decompose'):
            current.decompose()
        else:
            current.extract()
        current = next_tag
        
    articles_html = ""
    for article in articles:
        # [MODIFIED] Correctly access data from sqlite3.Row object (like a dictionary)
        snippet_text = article['snippet'] or ""
        headline_text = article['headline'] or "No headline"
        link_url = article['link'] or "#"
        source_name = article['source'] or "Unknown Source"
        
        date_display = (article['article_date'] or "").strip()
        if not date_display:
            try:
                scraped_dt = datetime.fromisoformat(article['scraped_date'].replace('Z', '+00:00'))
                date_display = scraped_dt.strftime("%d %b %Y")
            except (ValueError, KeyError):
                date_display = ""
        
        source_date_text = source_name
        if date_display:
             source_date_text += f" - <span class=\"article-date\">{date_display}</span>"
        
        articles_html += f"""
            <article class="news-item">
                <div class="text-content">
                    <a href="{link_url}" class="main-link" target="_blank" rel="noopener noreferrer">
                        <h3>{headline_text}</h3>
                        <p class="snippet">{snippet_text}</p>
                    </a>
                    <div class="source">{source_date_text}</div>
                </div>
            </article>
        """
        
    start_tag.insert_after(BeautifulSoup(articles_html, "html.parser"))
    
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
    
    scrapers = [
        scrape_tea_and_coffee_news,
        scrape_bbc_news,
        scrape_euronews
    ]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=100) 
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            for scraper_func in scrapers:
                try:
                    scraped_data = scraper_func(page)
                    all_scraped_articles.extend(scraped_data)
                except Exception as e:
                    print(f"Error running scraper {scraper_func.__name__}: {e}")
                time.sleep(random.uniform(2, 5)) 

            browser.close()
    except Exception as e:
        print(f"Playwright initialization or execution error: {e}")
        return

    if not all_scraped_articles:
        print("\nNo articles were successfully scraped in this run.")
    
    new_articles_count = 0
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

    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
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

    if all_db_articles:
        inject_html(all_db_articles)
    else:
        print("No articles retrieved from the database for injection.")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()