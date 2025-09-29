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

# Import for anti-bot detection evasion
try:
    # Ensure you have installed this: pip install playwright-stealth
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    print("\n*** NOTE: playwright-stealth not installed. Scrapes may be less reliable, especially on Bing.")
    print("Please install it: pip install playwright-stealth ***\n")

# Fuzzywuzzy setup
try:
    # Ensure you have installed this: pip install fuzzywuzzy python-Levenshtein
    from fuzzywuzzy import fuzz
    FUZZY_INSTALLED = True
except ImportError:
    print("Warning: fuzzywuzzy not found. Deduplication will be less effective.")
    FUZZY_INSTALLED = False
    class fuzz:
        @staticmethod
        def ratio(s1, s2):
            return 100 if s1 == s2 else 0

DB_FILE = "news.db"
HTML_FILE = "news.html"
MAX_PAGES_PER_SOURCE = 5 
BING_TARGET_ARTICLES = 200 # Target count for Bing News

# Configuration for stability
SELECTOR_TIMEOUT = 35000 
NAVIGATION_TIMEOUT = 90000 # 1.5 minutes
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
        
    # Clean the link (remove query/tracking parameters)
    parsed_url = urlparse(link)
    if parsed_url.scheme and parsed_url.netloc:
        clean_link = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
    else:
        clean_link = link

    try:
        cursor = conn.cursor()
        # Check if the exact link or the cleaned link path exists in the database 
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
        "Euronews": {"selectors": ['#didomi-notice-agree-button', 'button:has-text("Agree and close")']},
        "Tea & Coffee Trade Journal": {"selectors": ['button:has-text("Accept")']},
        "World Tea News": {"selectors": ['button[id="ketch-banner-button-primary"]']},
        # Expanded Bing News selectors (covering various regional IDs and labels)
        "Bing News": {"selectors": [
            'button#bnp_btn_accept', 'button[id*="accept"]', 
            'button:has-text("Accept all")', 'button:has-text("Accept")', 
            'button:has-text("Agree")', 'button:has-text("I agree")',
            'label[for="bnp_cookie_all"]', 'label:has-text("Accept")'
            ]},
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
        
        initial_timeout = 15000 # Give ample time for banners

        for i in range(3): # Try up to 3 times for sequential banners
            button = page.locator(combined_selector).first
            try:
                timeout_val = initial_timeout if i == 0 else 5000
                button.wait_for(state='visible', timeout=timeout_val) 
                print(f"  [Consent] Found banner. Clicking button/label.")
                # force=True is important for labels or obscured elements
                button.click(timeout=5000, force=True)
                # Wait for the button to potentially disappear
                try:
                     button.wait_for(state="hidden", timeout=5000)
                except PlaywrightTimeoutError:
                     pass # Continue even if the button doesn't hide, as long as the click succeeded
                
                banner_handled = True
                time.sleep(random.uniform(1.5, 3)) # Randomized pause
            except PlaywrightTimeoutError:
                break 
        
        if not banner_handled:
            print(f"  [Consent] No automatic consent found for {source_name}. Please click manually if present.")

        return banner_handled
            
    except Exception as e:
        print(f"  [Consent] Error during {source_name} consent handling: {e}")
        return False

# =============================================================================
# SCRAPER FUNCTIONS
# =============================================================================

# (scrape_tea_and_coffee_news, scrape_euronews, scrape_world_tea_news remain unchanged, 
# but are included here for completeness)

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
                # Filter out ads/sponsored content
                if item.locator(r"text=/sponsored|advertisement|AD\s*\|/i").count() > 0 or item.locator('h3 a').count() == 0: continue
                
                headline = item.locator('h3 a').first.inner_text()
                link = item.locator('h3 a').first.get_attribute('href')
                snippet_el = item.locator('div.articleExcerpt')
                snippet = snippet_el.inner_text().strip() if snippet_el.count() > 0 else ""
                
                # Clean up snippet if it starts with "NEWS"
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
                # Wait for the loading indicator to disappear
                page.locator('div.facetwp-loading').wait_for(state="hidden", timeout=20000)
                time.sleep(random.uniform(2, 5)) # Randomized wait after load
            except PlaywrightTimeoutError: print("  Pagination timed out. Stopping."); break
        else: print("  No 'Next' button found. Stopping."); break
    print(f"  Found {len(articles)} articles from {source_name}.")
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
        
    # Iterate through articles, skipping sponsored ones
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
            
            # Euronews stores the exact date in the 'datetime' attribute
            article_date = date_el.get_attribute('datetime') if date_el.count() > 0 else ""
            
            if headline and link: articles.append({"headline": headline.strip(), "snippet": snippet.strip(), "source": source_name, "link": urljoin(url, link), "article_date": article_date.strip()})
        except Exception as e:
            print(f"  Could not process an item: {e}")
    print(f"  Found {len(articles)} articles from {source_name}.")
    return articles

def scrape_world_tea_news(page):
    """Scrapes World Tea News using JSON-LD extraction with pagination."""
    source_name = "World Tea News"
    base_url = "https://www.worldteanews.com/whats-brewing"
    print(f"Scraping {source_name} (Using JSON-LD + Pagination)...")
    
    all_articles = []

    # Drupal pagination (page=0 is the first page, page=1 the second, etc.)
    for page_num in range(MAX_PAGES_PER_SOURCE):
        
        if page_num == 0:
            current_url = base_url
        else:
            current_url = f"{base_url}?page={page_num}"
            
        print(f"  Processing page {page_num + 1}/{MAX_PAGES_PER_SOURCE} (URL: {current_url})...")

        try:
            # Navigate to the page.
            response = page.goto(current_url, timeout=NAVIGATION_TIMEOUT, wait_until=NAVIGATION_WAIT_STRATEGY)
            
            # Handle consent banners/popups only on the first page load
            if page_num == 0:
                handle_consent(page, source_name)

            # Check if the page loaded successfully
            if response and response.status >= 400:
                 print(f"  [INFO] Page returned status {response.status}. Assuming end of results.")
                 break
            
            # Get the full HTML content
            html_content = page.content()
            
            # Use Regex to find all script tags of type application/ld+json
            pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)
            matches = pattern.findall(html_content)
            
            page_articles_data = []
            json_ld_found = False
            
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    # Look for the specific JSON-LD structure (ItemList)
                    if isinstance(data, dict) and data.get("@type") == "ItemList" and "itemListElement" in data:
                        json_ld_found = True
                        for item in data["itemListElement"]:
                            # Robust checking of the nested structure
                            if isinstance(item, dict) and item.get("item") and isinstance(item.get("item"), dict) and item["item"].get("@type") == "Article":
                                article = item["item"]
                                headline = article.get("name")
                                link = article.get("url")
                                snippet = article.get("description")
                                # Attempt to extract datePublished if present
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

# UPDATED FUNCTION: Implemented CSP-Compliant methods (Native Scrolling and Manual Polling)
def scrape_bing_news(page):
    """Scrapes Bing News search results using infinite scroll (CSP compliant)."""
    source_fallback_name = "Bing News"
    url = "https://www.bing.com/news/search?q=tea&FORM=HDRSC7"
    print(f"Scraping {source_fallback_name} (Infinite Scroll)...")
    
    articles = []
    processed_links = set()
    MAX_STAGNATION = 5 
    NEWS_CARD_SELECTOR = '.news-card'
    SCROLL_WAIT_TIMEOUT = 10 # Time (seconds) to wait/poll for new content after scroll

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
        handle_consent(page, "Bing News")

        # Wait for the main news container
        page.wait_for_selector('div#algocore', timeout=SELECTOR_TIMEOUT)

    except Exception as e:
        print(f"  [ERROR] Initial load or consent failed for {source_fallback_name}: {e}")
        save_debug_files(page, "debug_BingNews_InitialLoad")
        return articles

    stagnation_count = 0
    
    # --- Infinite Scroll Loop ---
    while len(articles) < BING_TARGET_ARTICLES:
        
        # 1. Capture current count of cards (before extraction/scroll)
        initial_card_count = page.locator(NEWS_CARD_SELECTOR).count()
        
        # 2. Extract data from visible cards
        # We must re-query the locator on every loop iteration to get fresh elements
        current_cards = page.locator(NEWS_CARD_SELECTOR).all()
        
        new_items_found = 0
        # Iterate over all cards currently on the page
        for card in current_cards:
            try:
                # --- Data Extraction ---
                title_el = card.locator('a.title').first
                if title_el.count() == 0: continue

                headline = title_el.inner_text().strip()
                
                # Prioritize the 'data-url' attribute for the direct link
                link = title_el.get_attribute('data-url')
                if not link:
                    # Fallback to the Bing redirect link ('href')
                    link = title_el.get_attribute('href')

                # CRITICAL CHECK: Skip if already processed in this session
                if not link or link in processed_links:
                    continue

                # Snippet
                snippet_el = card.locator('.snippet').first
                snippet = snippet_el.inner_text().strip() if snippet_el.count() > 0 else ""

                # Original Source (e.g., Reuters, Bloomberg) - Robust extraction
                original_source = ""
                # Attempt 1: Modern Bing layout using data-testid and aria-label
                provider_container = card.locator('[data-testid="news-source-container"] > div').first
                if provider_container.count() > 0:
                        aria_label = provider_container.get_attribute('aria-label')
                        if aria_label and aria_label.startswith("Provider: "):
                            original_source = aria_label.replace("Provider: ", "").strip()
                
                # Attempt 2: Fallback using structural selector
                if not original_source:
                    source_link_el = card.locator('div.source > a').first
                    if source_link_el.count() > 0:
                        original_source = source_link_el.inner_text().strip()

                # Date/Time
                date_el = card.locator('.time').first
                article_date = date_el.inner_text().strip() if date_el.count() > 0 else ""

                # --- Data Storage ---
                if headline and link:
                    display_source = original_source if original_source else source_fallback_name
                    
                    articles.append({
                        "headline": headline, "snippet": snippet, "source": display_source,
                        "link": link, "article_date": article_date
                    })
                    processed_links.add(link)
                    new_items_found += 1

                    if len(articles) >= BING_TARGET_ARTICLES:
                        break

            except Exception as e:
                # Suppress individual card errors and continue
                pass

        # 3. Check if target reached after extraction loop
        if len(articles) >= BING_TARGET_ARTICLES:
            print(f"  [INFO] Target article count ({BING_TARGET_ARTICLES}) reached.")
            break

        # 4. Stagnation Check (Initial)
        if new_items_found > 0:
            stagnation_count = 0
            print(f"  [INFO] Current article count: {len(articles)}. Scrolling for more...")
        # If new_items_found is 0, we proceed to scroll/wait, and check stagnation after the wait.

        # 5. Scroll down (CSP Compliant Method)
        # Scroll the *last* element found into view. This triggers the infinite scroll loader.
        try:
            last_card = page.locator(NEWS_CARD_SELECTOR).last
            # Use Playwright's built-in method instead of direct JavaScript evaluation
            last_card.scroll_into_view_if_needed(timeout=5000)
        except PlaywrightTimeoutError:
            print("    [INFO] Timeout while trying to scroll last element into view.")
            # If scrolling fails, we count it towards stagnation later
            pass
        
        # 6. Active Wait for New Content (CSP Compliant Manual Polling)
        # Replaces page.wait_for_function() which caused the CSP error.
        
        start_wait_time = time.time()
        content_loaded = False
        
        # Poll the count manually in Python.
        while time.time() - start_wait_time < SCROLL_WAIT_TIMEOUT:
            try:
                # Check the count again
                current_count = page.locator(NEWS_CARD_SELECTOR).count()
                if current_count > initial_card_count:
                    content_loaded = True
                    break
                # Pause briefly before checking again
                time.sleep(0.5) 
            except Exception:
                # Handle potential navigation issues during polling
                break

        # 7. Stagnation Check (Post-Wait)
        if not content_loaded:
            # If the count didn't increase within the timeout
            stagnation_count += 1
            print(f"    [INFO] Timeout waiting for new content after scroll. Stagnation: {stagnation_count}/{MAX_STAGNATION}.")
        else:
             # Add a brief randomized pause after new content is detected
            time.sleep(random.uniform(1, 3))

        if stagnation_count >= MAX_STAGNATION:
            print("  [INFO] Reached max stagnation limit. Stopping.")
            break
            
    
    print(f"  Found {len(articles)} articles from Bing News aggregation.")
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

    # Locate injection markers
    start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
    end_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)
    
    # If markers are missing, reset the container
    if not start_tag:
        injection_point.clear() 
        injection_point.append(Comment(" START_NEWS "))
        start_tag = injection_point.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
        
    # Clear existing content
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
        
        # Date Formatting Logic (Prioritize article_date, fallback to scraped_date)
        date_display = ""
        article_date_str = article['article_date']
        
        # 1. Try formatting article_date if it exists
        if article_date_str and article_date_str.strip():
            try:
                # Attempt to parse ISO format first (common in JSON-LD/Euronews)
                dt = datetime.fromisoformat(article_date_str.strip().replace('Z', '+00:00'))
                date_display = dt.strftime("%d %b %Y")
            except ValueError:
                # Fallback to displaying the raw string if parsing fails (e.g., Bing uses relative dates like '1h ago')
                date_display = article_date_str.strip()

        # 2. If article_date failed or was empty, try scraped_date
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
        
        # Build the HTML structure
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
    
    # Insert the new HTML
    if start_tag:
        start_tag.insert_after(BeautifulSoup(articles_html, "html.parser"))
    
    # Write back to the file
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(str(soup))
        
    print(f"Successfully injected {len(articles)} articles into {HTML_FILE}.")

# =============================================================================
# MAIN EXECUTION (CSP Bypass Applied Here)
# =============================================================================

def main():
    """Main function to run all scrapers, update the database, and rebuild the HTML."""
    start_time = time.time()
    print(f"Starting scraper at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    initialize_database()

    all_scraped_articles = []
    
    # Updated list of scrapers including Bing News
    scrapers = [
        scrape_tea_and_coffee_news,
        scrape_euronews,
        scrape_world_tea_news,
        scrape_bing_news,
    ]

    try:
        with sync_playwright() as p:
            
            # --- DYNAMIC HEADLESS MODE ---
            # Check if running in GitHub Actions (CI environment variable is automatically set by GitHub)
            is_ci = os.getenv("CI") == "true"
            
            # Run headless if in CI (is_ci=True), otherwise keep the browser visible (is_ci=False)
            # Reduce slow_mo in CI as it speeds up execution and we cannot watch it.
            browser = p.chromium.launch(headless=is_ci, slow_mo=50 if is_ci else 150) 
            # -----------------------------
            
            # --- CONTEXT SETUP WITH CSP BYPASS ---
            # We set bypass_csp=True to mitigate strict CSP issues globally,
            # -------------------------------------
            
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
                # Check for existence before insertion
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
                        # Handle potential duplicates missed by article_exists
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
            # Sorting by scraped_date DESC ensures the newest finds are prioritized.
            cursor.execute("""
                SELECT headline, snippet, source, link, article_date, scraped_date
                FROM articles
                ORDER BY scraped_date DESC, id DESC
                LIMIT 500
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