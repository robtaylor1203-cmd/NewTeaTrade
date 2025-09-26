import sqlite3
from playwright.sync_api import sync_playwright
from datetime import datetime, timezone
from urllib.parse import urljoin
from fuzzywuzzy import fuzz
import requests
import time
import random
from bs4 import BeautifulSoup, Comment

DB_FILE = "news.db"
HTML_FILE = "news.html"

def initialize_database():
    """Creates the news table in the database if it doesn't exist."""
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
        print("Database initialized successfully.")

def article_exists(headline, link, conn):
    """Checks if an article already exists to prevent duplicates."""
    cursor = conn.cursor()
    cursor.execute("SELECT headline FROM articles WHERE link = ?", (link,))
    if cursor.fetchone():
        return True
    cursor.execute("SELECT headline FROM articles")
    for row in cursor.fetchall():
        if fuzz.ratio(headline, row[0]) > 90:
            return True
    return False

def scrape_tea_and_coffee_news(page):
    """Scrapes articles from teaandcoffee.net/news"""
    print("Scraping Tea & Coffee News...")
    url = "https://www.teaandcoffee.net/news/"
    page.goto(url, wait_until="networkidle", timeout=60000)

    articles = []
    for item in page.locator('article:has(div.articleExcerpt)').all():
        try:
            headline_element = item.locator('h3 a')
            headline = headline_element.inner_text()
            link = headline_element.get_attribute('href')
            snippet = item.locator('div.articleExcerpt').inner_text()
            article_date = item.locator('div.meta').inner_text()

            if headline and link:
                articles.append({
                    "headline": headline.strip(),
                    "snippet": snippet.strip(),
                    "source": "Tea & Coffee Trade Journal",
                    "link": urljoin(url, link),
                    "article_date": article_date
                })
        except Exception as e:
            print(f"Could not process an item on Tea & Coffee News: {e}")

    return articles

def main():
    """Main function to run all scrapers and update the database."""
    initialize_database()

    all_articles = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        page = browser.new_page()

        all_articles.extend(scrape_tea_and_coffee_news(page))

        if not all_articles:
            print("No articles were scraped. Saving debug files...")
            page.screenshot(path="debug_homepage.png", full_page=True)
            with open("debug_homepage.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Debug screenshot and HTML file have been saved.")

        browser.close()

    if not all_articles:
        print("\nNo articles were found in this run.")
        return

    new_articles_count = 0
    with sqlite3.connect(DB_FILE) as conn:
        for article in all_articles:
            if not article_exists(article['headline'], article['link'], conn):
                conn.execute("""
                    INSERT INTO articles (headline, snippet, source, link, scraped_date, article_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    article['headline'],
                    article['snippet'],
                    article['source'],
                    article['link'],
                    datetime.now(timezone.utc).isoformat(),
                    article.get('article_date')
                ))
                new_articles_count += 1

    print(f"\nScraping complete. Added {new_articles_count} new articles to the database.")

    # --- HTML Injection Logic ---
    print("Injecting articles into HTML...")
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT headline, snippet, source, link, article_date
            FROM articles
            ORDER BY
                CASE WHEN article_date IS NULL THEN 1 ELSE 0 END,
                article_date DESC,
                scraped_date DESC
        """)
        articles = cursor.fetchall()

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    
    start_tag = soup.find(string=lambda text: isinstance(text, Comment) and "START_NEWS" in text)
    end_tag = soup.find(string=lambda text: isinstance(text, Comment) and "END_NEWS" in text)
    
    if not start_tag or not end_tag:
        print("Could not find the start and end tags in the HTML file.")
        return
        
    for tag in start_tag.find_all_next():
        if tag == end_tag:
            break
        tag.decompose()
        
    articles_html = ""
    for article in articles:
        articles_html += f"""
            <article class="news-item">
                <div class="text-content">
                    <a href="{article['link']}" class="main-link" target="_blank" rel="noopener noreferrer">
                        <h3>{article['headline']}</h3>
                        <p class="snippet">{article['snippet']}</p>
                    </a>
                    <div class="source">{article['source']} - <span class="article-date">{article['article_date']}</span></div>
                </div>
            </article>
        """
        
    start_tag.insert_after(BeautifulSoup(articles_html, "html.parser"))
    
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(str(soup))
        
    print(f"Successfully injected {len(articles)} articles into {HTML_FILE}.")

if __name__ == "__main__":
    main()