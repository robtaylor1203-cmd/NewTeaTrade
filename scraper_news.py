import sqlite3
import json
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timezone
from urllib.parse import urljoin, unquote, urlparse
import time
import random
import os
from bs4 import BeautifulSoup, Comment
import re

# NEW IMPORT: Required for anti-bot detection evasion
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    print("\n*** WARNING: playwright-stealth not installed. Scrapes on protected sites (BBC, East African) are likely to fail.")
    print("Please install it: pip install playwright-stealth ***\n")

# Fuzzywuzzy setup
try:
    from fuzzywuzzy import fuzz
    FUZZY_INSTALLED = True
except ImportError:
    print("Warning: fuzzywuzzy not found. Deduplication will be less effective. Install with: pip install fuzzywuzzy python-Levenshtein")
    FUZZY_INSTALLED = False
    class fuzz:
        @staticmethod
        def ratio(s1, s2):
            return 100 if s1 == s2 else 0

DB_FILE = "news.db"
HTML_FILE = "news.html"
MAX_PAGES_PER_SOURCE = 5 

# Configuration for stability
SELECTOR_TIMEOUT = 35000 
# Increased navigation timeout globally due to very slow sites like East African
NAVIGATION_TIMEOUT = 120000 # 2 minutes
NAVIGATION_WAIT_STRATEGY = "domcontentloaded" 

# =============================================================================
# DATABASE MANAGEMENT & HELPERS
# =============================================================================

def initialize_database():
    """Creates the news table in the database if it doesn't exist."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY, headline TEXT NOT NULL, snippet TEXT,
                    source TEXT NOT NULL, link TEXT NOT NULL UNIQUE,
                    scraped_date TEXT NOT NULL, article_date TEXT
                )
            """)
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")

def article_exists(headline, link, conn):
    """Checks if an article already exists to prevent duplicates."""
    if not link:
        return False
        
    parsed_url = urlparse(link)
    if parsed_url.scheme and parsed_url.netloc:
        clean_link = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
    else:
        clean_link = link

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM articles WHERE link = ? OR link LIKE ?", (link, clean_link + '%',))
        if cursor.fetchone():
            return True
        
        if FUZZY_INSTALLED:
            cursor.execute("SELECT headline FROM articles")
            for row in cursor.fetchall():
                if fuzz.ratio(headline, row[0]) > 90:
                    return True
        return False
    except sqlite3.Error as e:
        print(f"Database check error: {e}")
        return False

def save_debug_files(page, prefix="debug"):
    """Saves a screenshot and the HTML source of the current page upon failure."""
    try:
        if not page or page.is_closed():
            print(f"  [DEBUG] Page was closed or invalid, could not save debug files for {prefix}.")
            return
            
        # Wait briefly for the 'load' event to minimize empty screenshots
        try:
            page.wait_for_load_state("load", timeout=5000)
        except PlaywrightTimeoutError:
            pass

        safe_prefix = "".join([c for c in prefix if c.isalpha() or c.isdigit() or c=='_']).rstrip()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"{safe_prefix}_{timestamp}_screenshot.png"
        html_path = f"{safe_prefix}_{timestamp}_source.html"
        
        try:
            page.screenshot(path=screenshot_path, full_page=True)
        except Exception as ss_e:
             print(f"  [DEBUG] Could not save screenshot {screenshot_path}: {ss_e}")

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
            
        print(f"  [DEBUG] Issue occurred. Saved debug files: {screenshot_path} (if successful) and {html_path}")
    except Exception as e:
        print(f"  [DEBUG] Could not save debug files: {e}")

def handle_consent(page, source_name):
    """Handles specific cookie consent banners using an iterative approach."""
    consent_config = {
        "BBC News": {"selectors": [
            'button[data-testid="accept-all"]', 'button:has-text("Accept additional cookies")', 
            'button[aria-label*="agree" i]', 'button:has-text("Yes, I agree")',
            'button:has-text("Accept recommended cookies")', '#bbcprivacy-continue-button', '#blq-global-http-consent-accept'
            ]},
        "Euronews": {"selectors": ['#didomi-notice-agree-button', 'button:has-text("Agree and close")']},
        "Tea & Coffee Trade Journal": {"selectors": ['button:has-text("Accept")']},
        "World Tea News": {"selectors": ['button[id="ketch-banner-button-primary"]']},
        "The East African": {"selectors": ['button:has-text("I ACCEPT")', 'button:has-text("I AGREE")']},
    }
    
    # Specific handling for World Tea News survey pop-up (iframe)
    if source_name == "World Tea News":
        try:
            print("  [Consent] Checking for World Tea News survey pop-up (Usabilla)...")
            page.wait_for_selector('iframe[title="Usabilla Feedback Form"]', timeout=5000, state="visible")
            iframe = page.frame_locator('iframe[title="Usabilla Feedback Form"]')
            close_button = iframe.locator('a[aria-label="Close"]')
            close_button.wait_for(state='visible', timeout=5000)
            print("  [Consent] Found survey pop-up. Closing it.")
            close_button.click()
            time.sleep(random.uniform(1, 2))
        except PlaywrightTimeoutError:
            pass
        except Exception as e:
            print(f"  [Consent] Error closing survey pop-up: {e}")

    # General cookie banner handling
    config = consent_config.get(source_name)
    if not config: return False
    
    combined_selector = ", ".join(config["selectors"])
    
    try:
        print(f"  [Consent] Checking for {source_name} banners...")
        banner_handled = False
        
        # Increased initial wait for very slow sites like The East African
        initial_timeout = 20000 if source_name == "The East African" else 10000

        for i in range(3): # Try up to 3 times for sequential banners
            button = page.locator(combined_selector).first
            try:
                timeout_val = initial_timeout if i == 0 else 5000
                button.wait_for(state='visible', timeout=timeout_val) 
                print(f"  [Consent] Found banner. Clicking button.")
                button.click(timeout=5000, force=True)
                button.wait_for(state="hidden", timeout=10000)
                banner_handled = True
                time.sleep(random.uniform(1.5, 3)) # Randomized pause
            except PlaywrightTimeoutError:
                break 
        
        if not banner_handled:
            print(f"  [Consent] No automatic consent found for {source_name}.")

        return banner_handled
            
    except Exception as e:
        print(f"  [Consent] Error during {source_name} consent handling: {e}")
        return False

# =============================================================================
# SCRAPER FUNCTIONS
# =============================================================================

def scrape_tea_and_coffee_news(page):
    source_name = "Tea & Coffee Trade Journal"
    url = "https://www.teaandcoffee.net/news/"
    print(f"Scraping {source_name}...")
    articles = []
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, source_name)
        page.wait_for_selector('div.flex.facetwp-template', state='visible', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  [ERROR] Initial load failed for {source_name}: {e}")
        if isinstance(e, PlaywrightTimeoutError): save_debug_files(page, "debug_TC_InitialLoad")
        return articles
        
    for page_num in range(1, MAX_PAGES_PER_SOURCE + 1):
        print(f"  Processing page {page_num}...")
        
        try:
            page.wait_for_selector('div.flex.facetwp-template > article.row3', state='visible', timeout=20000)
        except PlaywrightTimeoutError:
            print("  [WARNING] Timeout waiting for articles on this page. Stopping pagination.")
            break
            
        for item in page.locator('div.flex.facetwp-template > article.row3').all():
            try:
                if item.locator(r"text=/sponsored|advertisement|AD\s*\|/i").count() > 0 or item.locator('h3 a').count() == 0: continue
                headline = item.locator('h3 a').first.inner_text()
                link = item.locator('h3 a').first.get_attribute('href')
                snippet_el = item.locator('div.articleExcerpt')
                snippet = snippet_el.inner_text().strip() if snippet_el.count() > 0 else ""
                if snippet.upper().startswith("NEWS"): snippet = snippet[4:].strip()
                date_el = item.locator('div.meta')
                article_date = date_el.inner_text().strip() if date_el.count() > 0 else ""
                if headline and link: articles.append({"headline": headline.strip(), "snippet": snippet, "source": source_name, "link": urljoin(url, link), "article_date": article_date})
            except Exception as e:
                print(f"  Could not process an item: {e}")
        if page_num == MAX_PAGES_PER_SOURCE: break
        
        # Pagination
        next_button = page.locator('a.facetwp-page.next')
        if next_button.count() > 0 and next_button.is_visible():
            print("  Navigating to next page...")
            next_button.click()
            try: 
                page.locator('div.facetwp-loading').wait_for(state="hidden", timeout=20000)
                time.sleep(random.uniform(2, 5)) # Randomized wait after load
            except PlaywrightTimeoutError: print("  Pagination timed out. Stopping."); break
        else: print("  No 'Next' button found. Stopping."); break
    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

# UPDATED FUNCTION: Relies on Stealth for anti-bot measures
def scrape_bbc_news(page):
    """Scrapes BBC News search results for 'tea industry'."""
    source_name = "BBC News"
    url = "https://www.bbc.co.uk/search?q=tea+industry&filter=news&s=latest"
    print(f"Scraping {source_name} (Using Search Approach)...")
    articles = []

    try:
        # Using domcontentloaded is usually sufficient if stealth is working
        page.goto(url, timeout=NAVIGATION_TIMEOUT, wait_until="domcontentloaded")
        
        handle_consent(page, source_name)
        
        time.sleep(random.uniform(2, 4))
        
        article_selector = 'div[data-testid="liverpool-card"]'
        
        try:
            # Wait for the first result card to appear (increased timeout)
            page.wait_for_selector(article_selector, timeout=45000)
        except PlaywrightTimeoutError:
             print(f"  [ERROR] Timeout waiting for search results ({article_selector}). Anti-bot measures might still be active despite stealth.")
             save_debug_files(page, f"debug_{source_name}_ResultsTimeout")
             return articles

        # Define selectors for extraction
        headline_link_selector = 'a[data-testid="card-headline"]'
        snippet_selector = 'p[data-testid="card-description"]'
        date_selector = 'span[data-testid*="card-metadata-last"]' 

        # Find all article elements
        article_elements = page.query_selector_all(article_selector)
        
        if not article_elements:
            print(f"  [WARNING] No articles found on {source_name} despite selector presence.")
            save_debug_files(page, f"debug_{source_name}_NoArticles")
            return articles

        processed_links = set()

        for article_element in article_elements:
            try:
                headline_link_el = article_element.query_selector(headline_link_selector)
                snippet_el = article_element.query_selector(snippet_selector)
                date_el = article_element.query_selector(date_selector)

                if headline_link_el:
                    headline = headline_link_el.inner_text().strip()
                    link = headline_link_el.get_attribute("href")
                    snippet = snippet_el.inner_text().strip() if snippet_el else ""
                    article_date = date_el.inner_text().strip() if date_el else ""
                    
                    # Clean up the date string
                    if article_date.lower().startswith("last updated"):
                        article_date = article_date[12:].strip()
                    elif article_date.lower().startswith("updated"):
                        article_date = article_date[7:].strip()

                    # Ensure the link is absolute
                    if link and not link.startswith('http'):
                        link = urljoin("https://www.bbc.co.uk", link)

                    # Basic filter to ensure it looks like a news article link (filters some audio/video programme links)
                    if headline and link and ("bbc.co.uk/news/" in link or "bbc.com/news/" in link):
                        if link in processed_links:
                            continue
                        processed_links.add(link)
                        
                        articles.append({
                            "headline": headline, "snippet": snippet, "source": source_name,
                            "link": link, "article_date": article_date
                        })

            except Exception as e:
                print(f"  [ERROR] Error processing an individual article link: {e}")
                continue

        print(f"  Found {len(articles)} articles from {source_name}.")
        return articles

    except PlaywrightTimeoutError:
        print(f"  [ERROR] Navigation timeout while processing {source_name}.")
        save_debug_files(page, f"debug_{source_name}_NavTimeout")
    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred while scraping {source_name}: {e}")
        save_debug_files(page, f"debug_{source_name}_Error")
    return articles


def scrape_euronews(page):
    source_name = "Euronews"
    url = "https://www.euronews.com/tag/tea"
    print(f"Scraping {source_name}...")
    articles = []
    try:
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, source_name)
        page.wait_for_selector('section[data-block="listing"]', state='visible', timeout=SELECTOR_TIMEOUT)
    except Exception as e:
        print(f"  [ERROR] Initial load failed for {source_name}: {e}")
        if isinstance(e, PlaywrightTimeoutError): save_debug_files(page, "debug_Euronews")
        return articles
        
    for item in page.locator('article.the-media-object:not(:has-text("In partnership with"))').all():
        try:
            headline_el = item.locator('h3.the-media-object__title')
            link_el = item.locator('a.the-media-object__link')
            if headline_el.count() == 0 or link_el.count() == 0: continue
            headline = headline_el.first.inner_text()
            link = link_el.first.get_attribute('href')
            snippet_el = item.locator('div.the-media-object__description')
            snippet = snippet_el.inner_text() if snippet_el.count() > 0 else ""
            date_el = item.locator('div.the-media-object__date > time')
            article_date = date_el.get_attribute('datetime') if date_el.count() > 0 else ""
            if headline and link: articles.append({"headline": headline.strip(), "snippet": snippet.strip(), "source": source_name, "link": urljoin(url, link), "article_date": article_date.strip()})
        except Exception as e:
            print(f"  Could not process an item: {e}")
    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_world_tea_news(page):
    source_name = "World Tea News"
    base_url = "https://www.worldteanews.com/whats-brewing"
    print(f"Scraping {source_name} (Using JSON-LD + Pagination)...")
    
    all_articles = []

    for page_num in range(MAX_PAGES_PER_SOURCE):
        
        if page_num == 0:
            current_url = base_url
        else:
            current_url = f"{base_url}?page={page_num}"
            
        print(f"  Processing page {page_num + 1}/{MAX_PAGES_PER_SOURCE} (URL: {current_url})...")

        try:
            response = page.goto(current_url, timeout=NAVIGATION_TIMEOUT, wait_until=NAVIGATION_WAIT_STRATEGY)
            
            if page_num == 0:
                handle_consent(page, source_name)

            if response and response.status >= 400:
                 print(f"  [INFO] Page returned status {response.status}. Assuming end of results.")
                 break
            
            html_content = page.content()
            pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)
            matches = pattern.findall(html_content)
            
            page_articles_data = []
            json_ld_found = False
            
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict) and data.get("@type") == "ItemList" and "itemListElement" in data:
                        json_ld_found = True
                        for item in data["itemListElement"]:
                            if isinstance(item, dict) and item.get("item") and isinstance(item.get("item"), dict) and item["item"].get("@type") == "Article":
                                article = item["item"]
                                headline = article.get("name")
                                link = article.get("url")
                                snippet = article.get("description")
                                article_date = article.get("datePublished", "")
                                
                                if headline and link:
                                    page_articles_data.append({
                                        "headline": headline.strip(),
                                        "link": link.strip(),
                                        "snippet": snippet.strip() if snippet else "",
                                        "source": source_name,
                                        "article_date": article_date.strip() if article_date else ""
                                    })
                        if page_articles_data:
                            break 
                except json.JSONDecodeError:
                    continue

            if not json_ld_found:
                if page_num == 0:
                    print(f"  [WARNING] Could not find JSON-LD data on the first page.")
                    save_debug_files(page, f"debug_{source_name}_NoJSONLD")
                else:
                    print("  [INFO] No JSON-LD data found on this page. Assuming end of results.")
                break

            all_articles.extend(page_articles_data)
            print(f"    [INFO] Found {len(page_articles_data)} articles on this page.")

            # Randomized delay
            time.sleep(random.uniform(3, 6))

        except PlaywrightTimeoutError:
            print(f"  [ERROR] Timeout while processing page {page_num + 1} of {source_name}.")
            save_debug_files(page, f"debug_{source_name}_Timeout_P{page_num+1}")
            break
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred while scraping {source_name}, page {page_num + 1}: {e}")
            save_debug_files(page, f"debug_{source_name}_Error_P{page_num+1}")
            break

    print(f"  Found {len(all_articles)} total articles from {source_name}.")
    return all_articles

# UPDATED FUNCTION: Reordered Consent and increased timeouts
def scrape_the_east_african(page):
    source_name = "The East African"
    url = "https://www.theeastafrican.co.ke/service/search/tea/4783234?query=tea&sortByDate=true"
    print(f"Scraping {source_name} (Cloudflare Protected)...")
    articles = []
    
    # --- CONFIGURATION ---
    LOAD_VERIFICATION_TIMEOUT = 90000    # 1.5 minutes to verify content loaded (very slow site)
    MANUAL_INTERVENTION_TIMEOUT = 120000 # 2 minutes for user to solve captcha

    main_content_selector = 'section.search-page-results'
    article_selector = 'article.nested-lazy-load'

    try:
        # 1. Navigation (Using NAVIGATION_WAIT_STRATEGY = domcontentloaded)
        page.goto(url, wait_until=NAVIGATION_WAIT_STRATEGY, timeout=NAVIGATION_TIMEOUT)

        # 2. Handle Consent Immediately (Crucial Change: Do this before waiting for content)
        print("  [INFO] Handling consent immediately (check browser window)...")
        handle_consent(page, source_name)

        # 3. Load Verification & Cloudflare Check
        try:
            print(f"  [INFO] Waiting up to {LOAD_VERIFICATION_TIMEOUT/1000}s for content to load...")
            page.wait_for_selector(main_content_selector, timeout=LOAD_VERIFICATION_TIMEOUT)
            print("  [CLOUDFLARE/LOAD] Content loaded successfully.")
        
        except PlaywrightTimeoutError:
            # Content didn't load. Check if it's Cloudflare.
            is_cloudflare = (page.locator("title:has-text('Just a moment...')").count() > 0 or 
                             "challenge-platform" in page.url or 
                             page.locator('text="Verify you are human"').count() > 0)

            if is_cloudflare:
                print("\n  *** [CLOUDFLARE ALERT] ***")
                print("  Cloudflare challenge detected.")
                print("  Please manually solve the captcha/checkbox in the browser window.")
                print(f"  Waiting up to {MANUAL_INTERVENTION_TIMEOUT/1000}s for manual intervention...")
                
                try:
                    # Wait for the content, indicating the challenge is passed.
                    page.wait_for_selector(main_content_selector, timeout=MANUAL_INTERVENTION_TIMEOUT)
                    print("  [CLOUDFLARE] Challenge passed. Continuing...")
                except PlaywrightTimeoutError:
                    print("  [ERROR] Timeout waiting for manual Cloudflare verification.")
                    save_debug_files(page, "debug_EastAfrican_CloudflareTimeout")
                    return articles
            else:
                # Timed out, but not Cloudflare.
                print(f"  [ERROR] Failed to load content within the timeout period. Site may be down or extremely slow.")
                save_debug_files(page, "debug_EastAfrican_LoadFail")
                return articles

        # 4. Final check for results
        try:
            # Wait for the first result to attach (confirms dynamic search finished)
            page.wait_for_selector(article_selector, state='attached', timeout=SELECTOR_TIMEOUT)
        except PlaywrightTimeoutError:
            print(f"  [WARNING] No articles found on {source_name} search page after waiting. Check selectors or search results.")
            save_debug_files(page, "debug_EastAfrican_NoArticles")
            return articles

    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred during navigation/bypass for {source_name}: {e}")
        save_debug_files(page, "debug_EastAfrican_Error")
        return articles

    # 5. Extraction
    for item in page.locator(article_selector).all():
        try:
            link_el_container = item.locator('div.text-content > a').first
            if link_el_container.count() == 0: continue

            link = link_el_container.get_attribute('href')
            
            headline_el = link_el_container.locator('h3').first
            if headline_el.count() == 0: continue
            headline = headline_el.inner_text().strip()

            snippet_el = link_el_container.locator('p').first 
            snippet = snippet_el.inner_text().strip() if snippet_el.count() > 0 else ""

            date_el = item.locator('div.text-content > p.date').first
            article_date = date_el.inner_text().strip() if date_el.count() > 0 else ""

            if headline and link:
                full_link = urljoin(url, link)
                articles.append({
                    "headline": headline, "snippet": snippet, "source": source_name,
                    "link": full_link, "article_date": article_date
                })
        except Exception as e:
            print(f"  Could not process an item: {e}")

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
    
    injection_point = soup.find('div', id='news-container')
    if not injection_point:
        print(f"Error: Could not find <div id='news-container'> in {HTML_FILE}. Cannot inject articles.")
        return

    start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
    end_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)
    
    if not start_tag:
        injection_point.clear() 
        injection_point.append(Comment(" START_NEWS "))
        start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
        
    if start_tag:
        current = start_tag.next_sibling
        while current and (not end_tag or current != end_tag):
            next_tag = current.next_sibling
            if hasattr(current, 'decompose'):
                current.decompose()
            elif current:
                current.extract()
            current = next_tag
        
    articles_html = ""
    for article in articles:
        # Access sqlite3.Row object using keys
        snippet_text = article['snippet'] or ""
        headline_text = article['headline'] or "No headline"
        link_url = article['link'] or "#"
        source_name = article['source'] or "Unknown Source"
        
        # Date Formatting Logic
        date_display = ""
        article_date_str = article['article_date']
        
        if article_date_str and article_date_str.strip():
            try:
                dt = datetime.fromisoformat(article_date_str.strip().replace('Z', '+00:00'))
                date_display = dt.strftime("%d %b %Y")
            except ValueError:
                date_display = article_date_str.strip()

        if not date_display:
            try:
                scraped_dt_str = article['scraped_date']
                if scraped_dt_str:
                    scraped_dt = datetime.fromisoformat(scraped_dt_str.replace('Z', '+00:00'))
                    date_display = scraped_dt.strftime("%d %b %Y")
            except (ValueError, TypeError, AttributeError):
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
    
    if start_tag:
        start_tag.insert_after(BeautifulSoup(articles_html, "html.parser"))
    
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(str(soup))
        
    print(f"Successfully injected {len(articles)} articles into {HTML_FILE}.")

# =============================================================================
# MAIN EXECUTION (Stealth Applied Here)
# =============================================================================

def main():
    """Main function to run all scrapers, update the database, and rebuild the HTML."""
    start_time = time.time()
    print(f"Starting scraper at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    initialize_database()

    all_scraped_articles = []
    
    scrapers = [
        scrape_tea_and_coffee_news,
        scrape_bbc_news,
        scrape_euronews,
        scrape_world_tea_news,
        scrape_the_east_african
    ]

    try:
        with sync_playwright() as p:
            # headless=False is essential for difficult sites and Cloudflare bypass.
            # slow_mo increased slightly to further mimic human interaction speed.
            browser = p.chromium.launch(headless=False, slow_mo=150) 
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080} # Set a realistic viewport
            )
            
            page = context.new_page()
            
            # --- APPLY STEALTH ---
            if STEALTH_AVAILABLE:
                print("[INFO] Applying playwright-stealth configuration...")
                stealth_sync(page)
            # ---------------------

            # Set default timeouts
            page.set_default_timeout(SELECTOR_TIMEOUT)
            page.set_default_navigation_timeout(NAVIGATION_TIMEOUT)

            for scraper_func in scrapers:
                print("-" * 40)
                try:
                    scraped_data = scraper_func(page)
                    if isinstance(scraped_data, list):
                        all_scraped_articles.extend(scraped_data)
                except Exception as e:
                    print(f"Critical Error running scraper {scraper_func.__name__}: {e}")
                    try:
                        save_debug_files(page, f"debug_CRASH_{scraper_func.__name__}")
                    except:
                        print("Could not save debug files post-crash.")
                # Polite, randomized delay between sources
                time.sleep(random.uniform(5, 10)) 

            browser.close()
    except Exception as e:
        print(f"Playwright initialization or execution error: {e}")
        return

    if not all_scraped_articles:
        print("\nNo articles were successfully scraped in this run.")
    
    # Database Insertion Phase
    new_articles_count = 0
    scraped_timestamp = datetime.now(timezone.utc).isoformat()

    print("-" * 40)
    print("Updating database...")
    try:
        with sqlite3.connect(DB_FILE) as conn:
            for article in all_scraped_articles:
                if not article_exists(article.get('headline'), article.get('link'), conn):
                    try:
                        conn.execute("""
                            INSERT INTO articles (headline, snippet, source, link, scraped_date, article_date)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            article.get('headline'), article.get('snippet'), article.get('source'),
                            article.get('link'), scraped_timestamp, article.get('article_date')
                        ))
                        new_articles_count += 1
                    except sqlite3.IntegrityError:
                        pass
            conn.commit()
    except sqlite3.Error as e:
        print(f"Database insertion error: {e}")

    print(f"Scraping complete. Added {new_articles_count} new articles to the database.")

    # HTML Generation Phase
    print("-" * 40)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()
            cursor.execute("""
                SELECT headline, snippet, source, link, article_date, scraped_date,
                       COALESCE(NULLIF(article_date, ''), scraped_date) as sort_date
                FROM articles
                ORDER BY sort_date DESC
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
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()