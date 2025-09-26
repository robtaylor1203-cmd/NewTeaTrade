import sqlite3
from datetime import datetime

DB_FILE = "news.db"
OUTPUT_HTML_FILE = "news.html"
TEMPLATE_HTML_FILE = "news_template.html" # We will create this template file

def fetch_articles_from_db():
    """Fetches all articles from the database, ordered by date."""
    print("Fetching articles from the database...")
    with sqlite3.connect(DB_FILE) as conn:
        # Make the cursor return rows that can be accessed by column name
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Order by article_date, putting articles with no date last.
        # Then, sort by the date we scraped them.
        cursor.execute("""
            SELECT headline, snippet, source, link, article_date
            FROM articles
            ORDER BY
                CASE WHEN article_date IS NULL THEN 1 ELSE 0 END,
                article_date DESC,
                scraped_date DESC
        """)
        articles = cursor.fetchall()
        print(f"Found {len(articles)} articles.")
        return articles

def create_article_html(article):
    """Creates an HTML block for a single article."""
    
    # --- Date Formatting ---
    # The date is stored in ISO format (e.g., "2025-09-26T10:00:00Z")
    # We will format it to be more readable, e.g., "26 September 2025"
    display_date = ""
    if article['article_date']:
        try:
            # Parse the ISO 8601 date format
            date_obj = datetime.fromisoformat(article['article_date'].replace('Z', '+00:00'))
            display_date = date_obj.strftime("%d %B %Y")
        except ValueError:
            # If the date format is unexpected, just display it as is.
            display_date = article['article_date']

    # Using an f-string to build the HTML block. This matches the style in style.css
    return f"""
        <article class="news-item">
            <div class="text-content">
                <a href="{article['link']}" class="main-link" target="_blank" rel="noopener noreferrer">
                    <h3>{article['headline']}</h3>
                    <p class="snippet">{article['snippet']}</p>
                </a>
                <div class="source">{article['source']} - <span class="article-date">{display_date}</span></div>
            </div>
        </article>
    """

def build_html_page():
    """Builds the final news.html page."""
    articles = fetch_articles_from_db()
    
    if not articles:
        print("No articles to build. Exiting.")
        return

    print("Generating HTML for each article...")
    all_articles_html = "".join([create_article_html(article) for article in articles])

    # Read the template file
    try:
        with open(TEMPLATE_HTML_FILE, "r", encoding="utf-8") as f:
            template_content = f.read()
    except FileNotFoundError:
        print(f"ERROR: Template file '{TEMPLATE_HTML_FILE}' not found. Please create it.")
        return
        
    # Replace the placeholder in the template with our generated HTML
    final_html = template_content.replace("", all_articles_html)

    # Write the new content to the output file
    with open(OUTPUT_HTML_FILE, "w", encoding="utf-8") as f:
        f.write(final_html)
        
    print(f"\n✅ Success! Your website has been updated.")
    print(f"   '{OUTPUT_HTML_FILE}' has been rebuilt with {len(articles)} articles.")


if __name__ == "__main__":
    build_html_page()