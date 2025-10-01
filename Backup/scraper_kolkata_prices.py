import pandas as pd
from playwright.sync_api import sync_playwright
import time
import random
import json

def scrape_jthomas_auctions():
    """
    Scrapes jthomasindia.com.
    V11 is "The Final Interceptor," correcting the JS selector and increasing
    timeouts for large data downloads.
    """
    url = "https://jthomasindia.com/auction_prices.php"
    all_data_dfs = []
    print("🚀 Launching Scraper (v11 - The Final Interceptor)...")

    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=False, slow_mo=50)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            stealth_script = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            page.add_init_script(stealth_script)

            print(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            print("Page loaded. Locating dropdown menu...")

            centre_dropdown_selector = 'body >> select >> nth=0'
            page.wait_for_selector(centre_dropdown_selector, timeout=30000)
            print("Successfully located the centre dropdown.")

            centre_options_locator = page.locator(f'{centre_dropdown_selector} >> option')
            centre_options = centre_options_locator.all()

            print(f"Found {len(centre_options) - 1} auction centers to process.")
            sale_dropdown_selector = 'body >> select >> nth=1'

            for centre_option in centre_options[1:]:
                centre_name = centre_option.inner_text()
                centre_value = centre_option.get_attribute('value')

                if not centre_value: continue

                print(f"\nProcessing Centre: {centre_name}")
                page.select_option(centre_dropdown_selector, value=centre_value)
                
                page.wait_for_selector(sale_dropdown_selector, timeout=15000)
                time.sleep(random.uniform(1.5, 3))

                sale_options_locator = page.locator(f'{sale_dropdown_selector} >> option')
                sale_options = sale_options_locator.all()
                for sale_option in sale_options[1:]:
                    sale_name = sale_option.inner_text()
                    sale_value = sale_option.get_attribute('value')
                    
                    if not sale_value: continue

                    try:
                        print(f"  -> Scraping Sale: {sale_name}")
                        page.select_option(sale_dropdown_selector, value=sale_value)
                        
                        response_url = "**/auction_prices_Details.php"
                        
                        print("     Clicking 'SHOW PRICES' and preparing to intercept data...")
                        
                        # --- CORRECTED CLICK & WAIT ---
                        # 1. First, we locate the button using Playwright's reliable method.
                        show_prices_button = page.get_by_role('button', name='SHOW PRICES')
                        
                        # 2. We set up the listener with a very long timeout for large data.
                        #    Timeout is in milliseconds (300,000 ms = 5 minutes).
                        with page.expect_response(response_url, timeout=300000) as response_info:
                            # 3. Now we click the button we already found.
                            show_prices_button.click()
                        
                        # 4. Wait for the response and process it.
                        response = response_info.value
                        data_json = response.json()
                        print("     ✅ Network data intercepted!")

                        auction_data = data_json.get('aaData', [])

                        if auction_data:
                            df = pd.DataFrame(auction_data)
                            df.columns = ['Lot No', 'Garden', 'Grade', 'Invoice', 'Packages', 'Price']
                            
                            df['auction_centre'] = centre_name
                            df['sale_no_scraped'] = sale_name
                            
                            all_data_dfs.append(df)
                            print(f"     ✅ Success! Processed {len(df)} rows from network response.")
                        else:
                            print("     ⚠️ Data was intercepted, but it was empty.")

                    except Exception as e:
                        print(f"     ❌ FAILED to scrape Sale {sale_name} for {centre_name}. Error: {e}")
                        continue

            if all_data_dfs:
                print("\nConsolidating all scraped data...")
                final_df = pd.concat(all_data_dfs, ignore_index=True)
                output_filename = "jthomas_all_auctions_intercepted.csv"
                final_df.to_csv(output_filename, index=False)
                print(f"\n🎉 COMPLETE! All data saved to '{output_filename}'.")
                print(f"   - Total rows scraped: {len(final_df)}")
            else:
                print("\nNo data was scraped.")

        except Exception as e:
            print(f"\n❌ A major error occurred: {e}")
        finally:
            if browser:
                print("Closing browser.")
                browser.close()

if __name__ == "__main__":
    scrape_jthomas_auctions()