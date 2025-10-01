import sqlite3
import pandas as pd
import logging
import os
import sys
import datetime
import altair as alt
import json

# Configuration
DB_FILE = "market_reports.db"
DATA_OUTPUT_DIR = "report_data" 
INDEX_FILE = os.path.join(DATA_OUTPUT_DIR, "mombasa_index.json")

# Configure logging to stdout for automation capture
logging.basicConfig(level=logging.INFO, format='ANALYZER: %(message)s', handlers=[logging.StreamHandler(sys.stdout)]) 
NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}
alt.data_transformers.disable_max_rows()

# =============================================================================
# Helper Functions (Database and Cleaning)
# =============================================================================

def connect_db():
    if not os.path.exists(DB_FILE):
        logging.error(f"Database file not found: {DB_FILE}"); sys.exit(1)
    try: return sqlite3.connect(DB_FILE)
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}"); sys.exit(1)

def clean_text_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.strip().str.upper()
        df[column_name] = df[column_name].replace(NOISE_VALUES, pd.NA)
    return df

def fetch_data(conn):
    # Fetches all data (minimal filtering)
    try:
        sales_df = pd.read_sql_query("SELECT * FROM auction_sales", conn)
        offers_df = pd.read_sql_query("SELECT * FROM auction_offers", conn)
        
        # Numeric conversion
        for df, cols in [(sales_df, ['price', 'quantity_kgs']), (offers_df, ['valuation_or_rp', 'quantity_kgs'])]:
            for col in cols:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

        # Text cleaning
        text_cols_common = ['mark', 'grade', 'broker', 'lot_number', 'sale_number', 'sale_date']
        for df in [sales_df, offers_df]:
            for col in text_cols_common: df = clean_text_column(df, col)
        sales_df = clean_text_column(sales_df, 'buyer')
        
        # Minimal filtering (Keys only)
        keys = ['broker', 'lot_number', 'sale_number', 'sale_date']
        sales_df = sales_df.dropna(subset=keys)
        offers_df = offers_df.dropna(subset=keys)
        
        # Ensure sale_number is valid
        sales_df = sales_df[sales_df['sale_number'] != 'UNKNOWN']
        offers_df = offers_df[offers_df['sale_number'] != 'UNKNOWN']
        
        return sales_df, offers_df
    except Exception as e:
        logging.error(f"Error fetching data: {e}", exc_info=True); sys.exit(1)

def prepare_sales_data(sales_df_raw):
    # Prepares data for detailed analysis (JIT filtering)
    required_cols = ['quantity_kgs', 'price', 'mark', 'grade', 'buyer']
    
    # Check if required columns exist before filtering (handles edge case of empty raw input)
    if not all(col in sales_df_raw.columns for col in required_cols):
         return pd.DataFrame()

    sales_df = sales_df_raw.dropna(subset=required_cols).copy()
    
    if 'quantity_kgs' in sales_df.columns and 'price' in sales_df.columns:
        sales_df = sales_df[(sales_df['quantity_kgs'] > 0) & (sales_df['price'] > 0)]
    
    # If filtering removes all rows, return an empty DF (with no columns)
    if sales_df.empty: return pd.DataFrame()
    
    sales_df['value_usd'] = sales_df['price'] * sales_df['quantity_kgs']
    return sales_df

# =============================================================================
# Analysis Functions (Adapted for per-week analysis)
# =============================================================================

def analyze_kpis(sales_df_week, sales_df_all):
    """Calculates KPIs for the specific week, including change vs previous week."""
    kpis = {}
    # FIX: Check if sales_df_week is empty or lacks essential columns (e.g., if prepare_sales_data returned empty DF)
    if sales_df_week.empty or 'quantity_kgs' not in sales_df_week.columns:
        kpis['TOTAL_VOLUME'] = "0"
        kpis['AVG_PRICE'] = "$0.00"
        kpis['PRICE_CHANGE'] = "N/A (No Sales)"
        kpis['PRICE_CHANGE_CLASS'] = 'neutral'
        return kpis

    # Current Week Metrics
    total_volume = sales_df_week['quantity_kgs'].sum()
    avg_price = sales_df_week['value_usd'].sum() / total_volume if total_volume > 0 else 0

    kpis['TOTAL_VOLUME'] = f"{total_volume:,.0f}"
    kpis['AVG_PRICE'] = f"${avg_price:.2f}"

    # Change vs Previous Week
    current_sale_number = sales_df_week['sale_number'].iloc[0]
    
    # Find the sale immediately preceding this one in the full dataset
    # Ensure sales_df_all is also valid before comparison
    if not sales_df_all.empty and 'quantity_kgs' in sales_df_all.columns:
        previous_sales = sales_df_all[sales_df_all['sale_number'] < current_sale_number]
        if not previous_sales.empty:
            previous_sale_number = previous_sales['sale_number'].max()
            prev_week_df = sales_df_all[sales_df_all['sale_number'] == previous_sale_number]
            
            prev_volume = prev_week_df['quantity_kgs'].sum()
            prev_avg_price = prev_week_df['value_usd'].sum() / prev_volume if prev_volume > 0 else 0

            if prev_avg_price > 0:
                change = ((avg_price - prev_avg_price) / prev_avg_price) * 100
                kpis['PRICE_CHANGE'] = f"{change:+.2f}%"
                # Classes used by front-end JavaScript for styling
                if change > 0.5: kpis['PRICE_CHANGE_CLASS'] = 'positive'
                elif change < -0.5: kpis['PRICE_CHANGE_CLASS'] = 'negative'
                else: kpis['PRICE_CHANGE_CLASS'] = 'neutral'

    if 'PRICE_CHANGE' not in kpis:
        kpis['PRICE_CHANGE'] = "N/A (First Sale)"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'

    return kpis

def analyze_forecast(sales_df_week_raw, offers_df_week):
    """Analyzes Sell-Through and Realization for the specific week."""
    forecast_data = {}

    # 1. Sell-Through
    offers_calc = offers_df_week.copy(); sales_calc = sales_df_week_raw.copy()
    
    # Ensure columns exist before creating keys
    if 'broker' in offers_calc.columns and 'lot_number' in offers_calc.columns:
        offers_calc['lot_key'] = offers_calc['broker'].astype(str) + '_' + offers_calc['lot_number'].astype(str)
        lots_offered = offers_calc['lot_key'].nunique()
    else:
        lots_offered = 0

    if 'broker' in sales_calc.columns and 'lot_number' in sales_calc.columns:
        sales_calc['lot_key'] = sales_calc['broker'].astype(str) + '_' + sales_calc['lot_number'].astype(str)
        lots_sold = sales_calc['lot_key'].nunique()
    else:
        lots_sold = 0

    sell_through_rate = (lots_sold / lots_offered) if lots_offered > 0 else 0
    
    forecast_data['SELL_THROUGH_RATE'] = f"{sell_through_rate:.2%}"

    # 2. Realization
    # Check if necessary columns exist before filtering
    if 'valuation_or_rp' in offers_df_week.columns and 'price' in sales_df_week_raw.columns:
        offers_with_valuation = offers_df_week[(offers_df_week['valuation_or_rp'] > 0)].copy()
        sales_with_price = sales_df_week_raw[(sales_df_week_raw['price'] > 0)].copy()

        if not offers_with_valuation.empty and not sales_with_price.empty:
            comparison_df = pd.merge(
                offers_with_valuation[['lot_number', 'broker', 'valuation_or_rp']],
                sales_with_price[['lot_number', 'broker', 'price']],
                on=['lot_number', 'broker']
            )
            if not comparison_df.empty:
                avg_realization = (comparison_df['price'] / comparison_df['valuation_or_rp']).mean()
                forecast_data['REALIZATION_RATE'] = f"{avg_realization:.2%}"

    if 'REALIZATION_RATE' not in forecast_data: forecast_data['REALIZATION_RATE'] = 'N/A'

    return forecast_data

# =============================================================================
# Chart and Table Generation (Altair Specs and List of Dicts)
# =============================================================================

def create_buyer_chart(sales_df_week):
    # Horizontal bar chart for top buyers this week
    # FIX: Check if empty or columns missing
    if sales_df_week.empty or 'buyer' not in sales_df_week.columns or 'value_usd' not in sales_df_week.columns: return {}
    
    top_buyers = sales_df_week.groupby('buyer')['value_usd'].sum().nlargest(10).reset_index()

    if top_buyers.empty: return {}

    chart = alt.Chart(top_buyers).mark_bar().encode(
        x=alt.X('value_usd:Q', title='Value (USD)'),
        y=alt.Y('buyer:N', title='Buyer', sort='-x'),
        color=alt.Color('value_usd:Q', scale=alt.Scale(scheme='yellowgreenblue'), legend=None),
        tooltip=[alt.Tooltip('buyer:N'), alt.Tooltip('value_usd:Q', format='$,.0f')]
    ).properties(title="Top 10 Buyers (by Value)")
    return chart.to_dict()

def create_grade_chart(sales_df_week):
    # Bar chart for key grade performance this week
    # FIX: Check if empty or columns missing (This is where the error occurred)
    if sales_df_week.empty or 'grade' not in sales_df_week.columns: return {}

    key_grades = ['BP1', 'PF1', 'PD', 'D1']
    key_grades_df = sales_df_week[sales_df_week['grade'].isin(key_grades)].copy()
    
    if key_grades_df.empty: return {}

    grade_analysis = key_grades_df.groupby('grade').agg(
        total_value=('value_usd', 'sum'), total_volume=('quantity_kgs', 'sum')
    ).reset_index()
    grade_analysis = grade_analysis[grade_analysis['total_volume'] > 0]
    
    if grade_analysis.empty: return {}

    grade_analysis['avg_price'] = grade_analysis['total_value'] / grade_analysis['total_volume']

    chart = alt.Chart(grade_analysis).mark_bar(color='#61dafb').encode(
        x=alt.X('grade:N', title='Grade', sort='-y'),
        y=alt.Y('avg_price:Q', title='Average Price (USD/kg)'),
        tooltip=[alt.Tooltip('grade:N'), alt.Tooltip('avg_price:Q', format='$.2f')]
    ).properties(title="Key CTC Grade Performance")
    return chart.to_dict()

def create_garden_table_data(sales_df_week):
    # Data for the top gardens table
    # FIX: Check if empty or columns missing
    if sales_df_week.empty or 'mark' not in sales_df_week.columns: return []

    garden_analysis = sales_df_week.groupby('mark').agg(
        total_volume=('quantity_kgs', 'sum'), total_value=('value_usd', 'sum')
    ).reset_index()
    
    # Ensure volume > 0 before division
    garden_analysis = garden_analysis[garden_analysis['total_volume'] > 0]
    if garden_analysis.empty: return []

    garden_analysis['avg_price'] = garden_analysis['total_value'] / garden_analysis['total_volume']
    
    # For a single week, show the top 10 by price realization
    top_gardens = garden_analysis.sort_values(by='avg_price', ascending=False).head(10)

    # Format for JSON output
    top_gardens = top_gardens.copy()
    top_gardens['total_volume'] = top_gardens['total_volume'].map('{:,.0f}'.format)
    top_gardens['avg_price'] = top_gardens['avg_price'].map('${:.2f}'.format)
    
    return top_gardens[['mark', 'total_volume', 'avg_price']].to_dict(orient='records')

# =============================================================================
# Main Processing Loop
# =============================================================================

def main():
    logging.info("Starting Mombasa Data Analysis (JSON Generation Mode)...")
    
    # Ensure output directory exists
    if not os.path.exists(DATA_OUTPUT_DIR): os.makedirs(DATA_OUTPUT_DIR)

    conn = connect_db()
    sales_df_raw, offers_df_raw = fetch_data(conn)
    
    # Prepare the full sales dataset for historical comparisons
    sales_df_all = prepare_sales_data(sales_df_raw)

    # Identify all unique sale weeks
    # Ensure columns exist before concatenation
    if 'sale_number' in sales_df_raw.columns and 'sale_number' in offers_df_raw.columns:
        all_weeks = pd.concat([sales_df_raw['sale_number'], offers_df_raw['sale_number']]).dropna().unique()
    else:
        all_weeks = []
    
    if len(all_weeks) == 0:
        logging.info("No data found."); return

    report_index = []

    # Process each week individually
    for week_number in sorted(all_weeks):
        logging.info(f"Processing Sale: {week_number}")
        
        # Filter data for the specific week
        sales_week_raw = sales_df_raw[sales_df_raw['sale_number'] == week_number]
        offers_week = offers_df_raw[offers_df_raw['sale_number'] == week_number]
        
        # Prepare sales data (this might return an empty DF if no sales occurred)
        sales_week = prepare_sales_data(sales_week_raw)
        
        # Determine the date
        week_date = "Unknown"
        if not sales_week_raw.empty: week_date = sales_week_raw['sale_date'].iloc[0]
        elif not offers_week.empty: week_date = offers_week['sale_date'].iloc[0]

        # Run analysis (functions now handle empty inputs gracefully)
        kpis = analyze_kpis(sales_week, sales_df_all)
        forecast = analyze_forecast(sales_week_raw, offers_week)
        
        # Combine KPIs and Forecast data
        if kpis:
            kpis.update(forecast)
        
        # Generate Charts and Tables (now robust against empty sales_week)
        charts = {
            'buyers': create_buyer_chart(sales_week),
            'grades': create_grade_chart(sales_week),
        }
        tables = {
            'gardens': create_garden_table_data(sales_week)
        }

        # Structure the report data
        report_data = {
            'metadata': {
                'sale_number': week_number, 'sale_date': week_date, 'location': 'Mombasa',
                'generated_at': datetime.datetime.now().isoformat()
            },
            'kpis': kpis,
            'charts': charts,
            'tables': tables
        }

        # Save the report JSON file
        filename = f"mombasa_{week_number.replace('-', '_')}.json"
        filepath = os.path.join(DATA_OUTPUT_DIR, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Add to index
            report_index.append({
                'sale_number': week_number, 'sale_date': week_date,
                'filename': filename, 'location': 'Mombasa'
            })
        except Exception as e:
            logging.error(f"Error saving JSON for {week_number}: {e}")

    # Save the index file (sorted descending)
    try:
        report_index.sort(key=lambda x: x['sale_number'], reverse=True)
        with open(INDEX_FILE, 'w') as f:
            json.dump(report_index, f, indent=2)
        logging.info(f"Generated index file: {INDEX_FILE}")
    except Exception as e:
        logging.error(f"Error saving index file: {e}")

    conn.close()
    logging.info("Analysis Complete.")

if __name__ == "__main__":
    main()