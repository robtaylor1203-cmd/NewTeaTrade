import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os

# Configuration
DB_FILE = "market_reports.db"
logging.basicConfig(level=logging.INFO, format='%(message)s')
sns.set_style("whitegrid")

NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}

def connect_db():
    if not os.path.exists(DB_FILE):
        logging.error(f"Database file not found: {DB_FILE}")
        exit(1)
    try:
        return sqlite3.connect(DB_FILE)
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}")
        exit(1)

def clean_text_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.strip().str.upper()
        df[column_name] = df[column_name].replace(NOISE_VALUES, pd.NA)
    return df

def fetch_data(conn):
    """V3: Fetches data and performs basic cleaning with minimal filtering."""
    try:
        sales_df = pd.read_sql_query("SELECT * FROM auction_sales", conn)
        offers_df = pd.read_sql_query("SELECT * FROM auction_offers", conn)
        
        # 1. Ensure key columns are numeric (errors='coerce' turns bad data into NaN)
        for df, cols in [(sales_df, ['price', 'quantity_kgs']), 
                         (offers_df, ['valuation_or_rp', 'quantity_kgs'])]:
            for col in cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 2. Clean text columns (Normalize and replace noise with pd.NA)
        text_cols_common = ['mark', 'grade', 'broker', 'lot_number', 'sale_number', 'sale_date']
        for df in [sales_df, offers_df]:
            for col in text_cols_common:
                df = clean_text_column(df, col)
        sales_df = clean_text_column(sales_df, 'buyer')
        
        # V3: 3. Minimal filtering: Only drop rows where essential KEYS are missing
        # These keys are required for linking and basic counting (Sell-Through)
        keys = ['broker', 'lot_number', 'sale_number', 'sale_date']

        sales_df = sales_df.dropna(subset=keys)
        offers_df = offers_df.dropna(subset=keys)
        
        # We do NOT filter on quantity, price, mark, or grade here. That is handled JIT.
        
        return sales_df, offers_df
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}", exc_info=True)
        exit(1)

# =============================================================================
# Analysis Functions (V3: Utilizing Just-In-Time filtering)
# =============================================================================

def prepare_sales_data_for_analysis(sales_df_raw):
    """V3: Helper to filter sales data specifically for volume/price/buyer/garden analysis."""
    # Requires valid quantity, price, and identifiers (Mark, Grade, Buyer)
    required_cols = ['quantity_kgs', 'price', 'mark', 'grade', 'buyer']
    
    # Drop rows missing these required columns and create a copy
    sales_df = sales_df_raw.dropna(subset=required_cols).copy()
    
    # Ensure values are positive
    sales_df = sales_df[(sales_df['quantity_kgs'] > 0) & (sales_df['price'] > 0)]

    if sales_df.empty:
        return pd.DataFrame() # Return empty DataFrame if no valid data remains

    # Calculate Value USD
    sales_df['value_usd'] = sales_df['price'] * sales_df['quantity_kgs']
    return sales_df


def analyze_overview(sales_df_raw):
    logging.info("\n--- 1. Market Overview ---")
    
    # V3: Prepare data specifically for this analysis
    sales_df = prepare_sales_data_for_analysis(sales_df_raw)

    if sales_df.empty:
        logging.info("No sales data with valid quantities, prices, and identifiers available.")
        return

    total_volume = sales_df['quantity_kgs'].sum()
    total_value = sales_df['value_usd'].sum()
    
    avg_price = total_value / total_volume if total_volume > 0 else 0
        
    distinct_sales = sales_df['sale_number'].nunique()
    sale_dates = pd.to_datetime(sales_df['sale_date'], errors='coerce').dropna()
    if sale_dates.empty:
         start_date, end_date = "N/A", "N/A"
    else:
        start_date = sale_dates.min().strftime('%Y-%m-%d')
        end_date = sale_dates.max().strftime('%Y-%m-%d')

    logging.info(f"Period: {start_date} to {end_date} (Sales Analyzed: {distinct_sales})")
    logging.info(f"Total Volume Sold: {total_volume:,.0f} kg")
    logging.info(f"Total Value (USD): ${total_value:,.2f}")
    logging.info(f"Average Price (USD/kg, Weighted): ${avg_price:.2f}")

def analyze_trends(sales_df_raw):
    logging.info("\n--- 2. Sales Trends Highlights ---")
    
    # V3: Prepare data
    sales_df = prepare_sales_data_for_analysis(sales_df_raw)
    if sales_df.empty:
        return

    trend_df = sales_df.groupby('sale_number').agg(
        volume_kg=('quantity_kgs', 'sum'),
        total_value=('value_usd', 'sum'),
        sale_date=('sale_date', 'first') 
    ).reset_index()
    
    trend_df['avg_price_usd'] = trend_df['total_value'] / trend_df['volume_kg']
    trend_df = trend_df.sort_values(by='sale_number')

    logging.info("Volume and Average Price by Sale Number:")
    print(trend_df[['sale_number', 'sale_date', 'volume_kg', 'avg_price_usd']].to_markdown(index=False))

    # Visualization
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    color = 'tab:blue'
    ax1.set_xlabel('Sale Number (Year-Week)')
    ax1.set_ylabel('Volume (kg)', color=color)
    ax1.bar(trend_df['sale_number'], trend_df['volume_kg'], color=color, alpha=0.6)
    ax1.tick_params(axis='y', labelcolor=color)
    plt.xticks(rotation=45, ha='right')
    
    ax2 = ax1.twinx() 
    color = 'tab:red'
    ax2.set_ylabel('Average Price (USD/kg)', color=color)
    ax2.plot(trend_df['sale_number'], trend_df['avg_price_usd'], color=color, marker='o', linewidth=2)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.grid(False)
    
    fig.tight_layout()
    plt.title('Mombasa Auction Trends: Volume vs. Price')
    plt.savefig('market_trends.png')
    logging.info("\nVisualization saved as 'market_trends.png'")
    
    # Insight Generation
    if len(trend_df) > 1:
        latest_sale = trend_df.iloc[-1]
        prev_sale = trend_df.iloc[-2]
        
        price_change = 0
        if prev_sale['avg_price_usd'] > 0:
            price_change = ((latest_sale['avg_price_usd'] - prev_sale['avg_price_usd']) / prev_sale['avg_price_usd']) * 100
            
        volume_change = 0
        if prev_sale['volume_kg'] > 0:
            volume_change = ((latest_sale['volume_kg'] - prev_sale['volume_kg']) / prev_sale['volume_kg']) * 100
        
        logging.info(f"\nTrend Insight: In the latest sale ({latest_sale['sale_number']}), the average price changed by {price_change:+.2f}% and volume changed by {volume_change:+.2f}%.")

def analyze_buyer_activity(sales_df_raw):
    logging.info("\n--- 3. Buyer Activity Analysis ---")

    # V3: Prepare data
    sales_df = prepare_sales_data_for_analysis(sales_df_raw)
    if sales_df.empty:
        return

    # Top Buyers Overall
    top_buyers = sales_df.groupby('buyer').agg(
        total_volume=('quantity_kgs', 'sum'),
        total_value=('value_usd', 'sum'),
    ).reset_index()

    # Recalculate avg price
    top_buyers['avg_price_paid'] = top_buyers['total_value'] / top_buyers['total_volume']
    top_buyers = top_buyers.sort_values(by='total_value', ascending=False).head(10)

    logging.info("Top 10 Buyers by Total Value (USD):")
    print(top_buyers.to_markdown(index=False))

    # Buyer activity Heatmap
    buyer_pivot = pd.pivot_table(sales_df, 
                                 values='value_usd', index='buyer', columns='sale_number', 
                                 aggfunc='sum', fill_value=0)
    
    top_buyers_list = top_buyers['buyer'].tolist()
    if not top_buyers_list: return
        
    buyer_pivot = buyer_pivot.loc[buyer_pivot.index.isin(top_buyers_list)]
    buyer_pivot = buyer_pivot.reindex(top_buyers_list)

    plt.figure(figsize=(12, 8))
    sns.heatmap(buyer_pivot, annot=True, fmt=".0f", cmap="YlGnBu", linewidths=.5)
    plt.title('Buyer Activity Heatmap (USD Value) by Sale Number')
    plt.xlabel('Sale Number'); plt.ylabel('Buyer')
    plt.tight_layout()
    plt.savefig('buyer_activity_heatmap.png')
    logging.info("\nVisualization saved as 'buyer_activity_heatmap.png'")

def analyze_grades_and_gardens(sales_df_raw):
    logging.info("\n--- 4. Grade and Garden Performance (Intelligent Links) ---")
    
    # V3: Prepare data
    sales_df = prepare_sales_data_for_analysis(sales_df_raw)
    if sales_df.empty:
        return
        
    # Key CTC Grades Analysis
    key_grades = ['BP1', 'PF1', 'PD', 'D1']
    key_grades_df = sales_df[sales_df['grade'].isin(key_grades)]

    if not key_grades_df.empty:
        grade_analysis = key_grades_df.groupby('grade').agg(
            total_volume=('quantity_kgs', 'sum'),
            total_value=('value_usd', 'sum'),
        ).reset_index()
        
        grade_analysis['avg_price'] = grade_analysis['total_value'] / grade_analysis['total_volume']
        
        logging.info("Performance of Key CTC Grades:")
        print(grade_analysis.sort_values(by='avg_price', ascending=False).to_markdown(index=False))

    # Top Gardens (Marks) by Price Realization
    garden_analysis = sales_df.groupby('mark').agg(
        total_volume=('quantity_kgs', 'sum'),
        total_value=('value_usd', 'sum'),
    ).reset_index()
    
    garden_analysis['avg_price'] = garden_analysis['total_value'] / garden_analysis['total_volume']
    
    VOLUME_THRESHOLD = 5000
    top_gardens_filtered = garden_analysis[garden_analysis['total_volume'] > VOLUME_THRESHOLD]
    
    if top_gardens_filtered.empty:
        logging.info(f"\nNo gardens met the {VOLUME_THRESHOLD}kg threshold. Showing top 5 overall by volume instead.")
        top_gardens_display = garden_analysis.sort_values(by='total_volume', ascending=False).head(5)
    else:
        logging.info(f"\nTop 10 Gardens (Marks) by Average Price Realization (Min {VOLUME_THRESHOLD/1000:.0f}k kg):")
        top_gardens_display = top_gardens_filtered.sort_values(by='avg_price', ascending=False).head(10)

    print(top_gardens_display.to_markdown(index=False))
    
    # Visualization: Price trends
    if not key_grades_df.empty:
        grade_trend_df = key_grades_df.groupby(['sale_number', 'grade']).agg(
            total_value=('value_usd', 'sum'),
            total_volume=('quantity_kgs', 'sum')
        ).reset_index()
        grade_trend_df['avg_price'] = grade_trend_df['total_value'] / grade_trend_df['total_volume']

        plt.figure(figsize=(12, 7))
        sns.lineplot(data=grade_trend_df, x='sale_number', y='avg_price', hue='grade', marker='o', style='grade')
        plt.title('Price Trends of Key CTC Grades')
        plt.xlabel('Sale Number'); plt.ylabel('Average Price (USD/kg)')
        plt.legend(title='Grade'); plt.xticks(rotation=45, ha='right'); plt.tight_layout()
        plt.savefig('grade_price_trends.png')
        logging.info("\nVisualization saved as 'grade_price_trends.png'")

def analyze_forecast(sales_df_raw, offers_df_raw):
    logging.info("\n--- 5. Forecast and Offer Analysis (Sell-Through and Realization) ---")
    
    # V3: Use the raw (minimally filtered) dataframes for counting and linking.

    # 1. Sell-Through Rate Analysis
    
    # V3 FIX: Use nunique() on a composite key for efficiency and to resolve FutureWarning.
    # Create composite keys (Broker_LotNumber)
    offers_calc = offers_df_raw.copy()
    sales_calc = sales_df_raw.copy()

    # Create the key safely
    offers_calc['lot_key'] = offers_calc['broker'].astype(str) + '_' + offers_calc['lot_number'].astype(str)
    sales_calc['lot_key'] = sales_calc['broker'].astype(str) + '_' + sales_calc['lot_number'].astype(str)
    
    # Count unique lots offered and sold per sale number
    offers_count = offers_calc.groupby('sale_number')['lot_key'].nunique().reset_index(name='lots_offered')
    sales_count = sales_calc.groupby('sale_number')['lot_key'].nunique().reset_index(name='lots_sold')
    
    # Merge using 'outer' join
    sell_through_df = pd.merge(offers_count, sales_count, on='sale_number', how='outer')
    sell_through_df = sell_through_df.fillna(0)

    # Calculate rate
    sell_through_df['sell_through_rate'] = sell_through_df.apply(
        lambda row: (row['lots_sold'] / row['lots_offered']) if row['lots_offered'] > 0 else 0, axis=1
    )

    logging.info("Sell-Through Rate by Sale Number:")
    print(sell_through_df.sort_values(by='sale_number').to_markdown(index=False))

    # 2. Price Realization Analysis
    
    # V3: Filter offers that specifically have a valuation (JIT)
    offers_with_valuation = offers_df_raw[
        (offers_df_raw['valuation_or_rp'].notna()) & (offers_df_raw['valuation_or_rp'] > 0)
    ].copy()

    # Filter sales that specifically have a price (JIT)
    sales_with_price = sales_df_raw[
        (sales_df_raw['price'].notna()) & (sales_df_raw['price'] > 0)
    ].copy()

    realization_summary = pd.DataFrame() # Initialize empty dataframe

    if offers_with_valuation.empty:
        logging.info("\nNo offers found with valuations/RP in the database.")
    elif sales_with_price.empty:
        logging.info("\nNo sales found with prices in the database.")
    else:
        # Merge on the essential keys
        offers_subset = offers_with_valuation[['sale_number', 'lot_number', 'broker', 'valuation_or_rp']]
        sales_subset = sales_with_price[['sale_number', 'lot_number', 'broker', 'price']]

        comparison_df = pd.merge(offers_subset, sales_subset, 
                                 on=['sale_number', 'lot_number', 'broker'])
        
        if comparison_df.empty:
            logging.info("\nCould not match any offers with valuations to corresponding sales records.")
        else:
            comparison_df['realization_rate'] = comparison_df['price'] / comparison_df['valuation_or_rp']

            # Summarize realization by sale
            realization_summary = comparison_df.groupby('sale_number').agg(
                avg_realization=('realization_rate', 'mean'),
                lots_compared=('lot_number', 'count')
            ).reset_index()

            logging.info("\nAverage Price Realization Rate (Sale Price / Valuation):")
            print(realization_summary.sort_values(by='sale_number').to_markdown(index=False))

    # 3. Forecasting Insights
    report_forecast_insights(sell_through_df, realization_summary)


def report_forecast_insights(sell_through_df, realization_summary):
    """Helper function to report insights."""
    logging.info("\nForecasting Insight:")
    
    # Analyze Sell-Through
    valid_sell_through = sell_through_df[sell_through_df['lots_offered'] > 0]
    if not valid_sell_through.empty:
        avg_sell_through = valid_sell_through['sell_through_rate'].mean()
        if avg_sell_through > 1.0:
             logging.warning(f"- Sell-Through ({avg_sell_through:.2%}): Anomaly (Sold > Offered). Data requires review (missing offer files?).")
        elif avg_sell_through > 0.95:
            logging.info(f"- Sell-Through ({avg_sell_through:.2%}): Very High. Indicates a strong seller's market.")
        elif avg_sell_through < 0.85:
            logging.info(f"- Sell-Through ({avg_sell_through:.2%}): Low/Selective. Indicates oversupply or weak demand.")
        else:
            logging.info(f"- Sell-Through ({avg_sell_through:.2%}): Moderate.")

    # Analyze Realization
    if not realization_summary.empty:
        avg_realization = realization_summary['avg_realization'].mean()
        if avg_realization > 1.02:
            logging.info(f"- Realization ({avg_realization:.2%}): Above Par. Prices exceeding valuations.")
        elif avg_realization < 0.98:
            logging.info(f"- Realization ({avg_realization:.2%}): Below Par. Prices failing to meet valuations.")
        else:
            logging.info(f"- Realization ({avg_realization:.2%}): At Par.")

# =============================================================================
# Main Execution
# =============================================================================

def main():
    logging.info("Starting Mombasa Auction Analysis (V3)...")
    conn = connect_db()
    # V3: Fetch minimally filtered data
    sales_df_raw, offers_df_raw = fetch_data(conn)

    # Run analysis, passing the raw dataframes
    analyze_overview(sales_df_raw)
    analyze_trends(sales_df_raw)
    analyze_buyer_activity(sales_df_raw)
    analyze_grades_and_gardens(sales_df_raw)
    analyze_forecast(sales_df_raw, offers_df_raw)

    conn.close()
    logging.info("\nAnalysis Complete. Visualizations saved as PNG files.")

if __name__ == "__main__":
    try:
        import matplotlib
        import seaborn
    except ImportError:
        logging.error("Please install required libraries: pip install matplotlib seaborn pandas")
        exit(1)
        
    main()