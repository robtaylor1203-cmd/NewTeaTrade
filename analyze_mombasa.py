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

# Configure logging
logging.basicConfig(level=logging.INFO, format='ANALYZER: %(message)s', handlers=[logging.StreamHandler(sys.stdout)]) 
NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}
alt.data_transformers.disable_max_rows()

# =============================================================================
# Helper Functions (Database and Cleaning)
# (These functions remain the same as the previous robust versions)
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

        sales_df = sales_df[sales_df['sale_number'] != 'UNKNOWN']
        offers_df = offers_df[offers_df['sale_number'] != 'UNKNOWN']

        return sales_df, offers_df
    except Exception as e:
        logging.error(f"Error fetching data: {e}", exc_info=True); sys.exit(1)

def prepare_sales_data(sales_df_raw):
    required_cols = ['quantity_kgs', 'price', 'mark', 'grade', 'buyer']

    if not all(col in sales_df_raw.columns for col in required_cols):
         return pd.DataFrame()

    sales_df = sales_df_raw.dropna(subset=required_cols).copy()

    if 'quantity_kgs' in sales_df.columns and 'price' in sales_df.columns:
        sales_df = sales_df[(sales_df['quantity_kgs'] > 0) & (sales_df['price'] > 0)]

    if sales_df.empty: return pd.DataFrame()

    sales_df['value_usd'] = sales_df['price'] * sales_df['quantity_kgs']
    return sales_df

# =============================================================================
# Analysis Functions (KPIs, Forecast, and Snapshot)
# =============================================================================

def analyze_kpis_and_forecast(sales_df_week, sales_df_all, sales_df_week_raw, offers_df_week):
    """Combines KPI calculation, forecast analysis, and snapshot generation."""
    kpis = {}
    tables = {'sell_through': [], 'realization': []}

    # 1. Sales KPIs
    if sales_df_week.empty or 'quantity_kgs' not in sales_df_week.columns:
        kpis['TOTAL_VOLUME'] = "0"; kpis['AVG_PRICE'] = "$0.00"
        kpis['PRICE_CHANGE'] = "N/A"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'
        kpis['PRICE_CHANGE_NUMERIC'] = 0
    else:
        total_volume = sales_df_week['quantity_kgs'].sum()
        avg_price = sales_df_week['value_usd'].sum() / total_volume if total_volume > 0 else 0
        kpis['TOTAL_VOLUME'] = f"{total_volume:,.0f}"; kpis['AVG_PRICE'] = f"${avg_price:.2f}"

        # Calculate Price Change vs Previous Week
        current_sale_number = sales_df_week['sale_number'].iloc[0]
        if not sales_df_all.empty and 'quantity_kgs' in sales_df_all.columns:
            previous_sales = sales_df_all[sales_df_all['sale_number'] < current_sale_number]
            if not previous_sales.empty:
                previous_sale_number = previous_sales['sale_number'].max()
                prev_week_df = sales_df_all[sales_df_all['sale_number'] == previous_sale_number]
                prev_volume = prev_week_df['quantity_kgs'].sum()
                prev_avg_price = prev_week_df['value_usd'].sum() / prev_volume if prev_volume > 0 else 0

                if prev_avg_price > 0:
                    change = ((avg_price - prev_avg_price) / prev_avg_price) * 100
                    kpis['PRICE_CHANGE_NUMERIC'] = change
                    kpis['PRICE_CHANGE'] = f"{change:+.2f}%"
                    # Classes are kept for potential subtle styling indicators (e.g., green/red text)
                    if change > 0.5: kpis['PRICE_CHANGE_CLASS'] = 'positive'
                    elif change < -0.5: kpis['PRICE_CHANGE_CLASS'] = 'negative'
                    else: kpis['PRICE_CHANGE_CLASS'] = 'neutral'

        if 'PRICE_CHANGE' not in kpis:
            kpis['PRICE_CHANGE'] = "N/A (First Sale)"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'; kpis['PRICE_CHANGE_NUMERIC'] = 0

    # 2. Forecast Analysis (Sell-Through)
    offers_calc = offers_df_week.copy(); sales_calc = sales_df_week_raw.copy()
    lots_offered = 0; lots_sold = 0

    if 'broker' in offers_calc.columns and 'lot_number' in offers_calc.columns:
        offers_calc['lot_key'] = offers_calc['broker'].astype(str) + '_' + offers_calc['lot_number'].astype(str)
        lots_offered = offers_calc['lot_key'].nunique()

    if 'broker' in sales_calc.columns and 'lot_number' in sales_calc.columns:
        sales_calc['lot_key'] = sales_calc['broker'].astype(str) + '_' + sales_calc['lot_number'].astype(str)
        lots_sold = sales_calc['lot_key'].nunique()

    sell_through_rate = (lots_sold / lots_offered) if lots_offered > 0 else 0
    kpis['SELL_THROUGH_RATE'] = f"{sell_through_rate:.2%}"
    kpis['SELL_THROUGH_RATE_RAW'] = sell_through_rate

    # Populate Sell-Through Table Data
    tables['sell_through'].append({'Metric': 'Lots Offered', 'Value': f"{lots_offered:,.0f}"})
    tables['sell_through'].append({'Metric': 'Lots Sold', 'Value': f"{lots_sold:,.0f}"})
    tables['sell_through'].append({'Metric': 'Rate', 'Value': kpis['SELL_THROUGH_RATE']})

    # 3. Forecast Analysis (Realization)
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
                kpis['REALIZATION_RATE'] = f"{avg_realization:.2%}"
                # Populate Realization Table Data
                tables['realization'].append({'Metric': 'Lots Compared', 'Value': f"{len(comparison_df):,.0f}"})
                tables['realization'].append({'Metric': 'Average Rate', 'Value': kpis['REALIZATION_RATE']})

    if 'REALIZATION_RATE' not in kpis:
        kpis['REALIZATION_RATE'] = 'N/A'
        tables['realization'].append({'Metric': 'Status', 'Value': 'Insufficient Data'})

    # 4. Snapshot Generation (Narrative)
    kpis['SNAPSHOT'] = generate_snapshot(kpis)

    return kpis, tables

def generate_snapshot(kpis):
    """Generates a narrative topline snapshot."""
    if kpis.get('TOTAL_VOLUME') == '0':
        return "Awaiting sales results. Offers published."

    price_change = kpis.get('PRICE_CHANGE_NUMERIC', 0)
    if price_change > 1.5: price_desc = "Prices significantly higher"
    elif price_change > 0.5: price_desc = "Prices firm to dearer"
    elif price_change < -1.5: price_desc = "Prices significantly lower"
    elif price_change < -0.5: price_desc = "Prices easier"
    else: price_desc = "Prices generally steady"

    sell_through_rate = kpis.get('SELL_THROUGH_RATE_RAW', 0)
    if sell_through_rate > 0.95: demand_desc = "Excellent absorption."
    elif sell_through_rate >= 0.85: demand_desc = "Good general demand."
    elif sell_through_rate >= 0.75: demand_desc = "Fair demand; selective buying."
    else: demand_desc = "Low demand; significant withdrawals."

    snapshot = f"{price_desc} ({kpis.get('PRICE_CHANGE', 'N/A')}). {demand_desc}"
    return snapshot

# =============================================================================
# Chart and Table Generation (Using default Altair colors)
# =============================================================================

def create_buyer_chart(sales_df_week):
    # Using default Altair scheme
    if sales_df_week.empty or 'buyer' not in sales_df_week.columns or 'value_usd' not in sales_df_week.columns: return {}
    top_buyers = sales_df_week.groupby('buyer')['value_usd'].sum().nlargest(10).reset_index()
    if top_buyers.empty: return {}
    chart = alt.Chart(top_buyers).mark_bar().encode(
        x=alt.X('value_usd:Q', title='Value (USD)'),
        y=alt.Y('buyer:N', title='Buyer', sort='-x'),
        # Let Altair choose the color based on the default theme
        color=alt.Color('value_usd:Q', legend=None),
        tooltip=[alt.Tooltip('buyer:N'), alt.Tooltip('value_usd:Q', format='$,.0f')]
    ).properties(title="Top 10 Buyers (by Value)")
    return chart.to_dict()

def create_grade_chart(sales_df_week):
    # Using default Altair blue
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

    # Let Altair choose the default bar color
    chart = alt.Chart(grade_analysis).mark_bar().encode(
        x=alt.X('grade:N', title='Grade', sort='-y'),
        y=alt.Y('avg_price:Q', title='Average Price (USD/kg)'),
        tooltip=[alt.Tooltip('grade:N'), alt.Tooltip('avg_price:Q', format='$.2f')]
    ).properties(title="Key CTC Grade Performance")
    return chart.to_dict()

def create_garden_table_data(sales_df_week):
    if sales_df_week.empty or 'mark' not in sales_df_week.columns: return []
    garden_analysis = sales_df_week.groupby('mark').agg(
        total_volume=('quantity_kgs', 'sum'), total_value=('value_usd', 'sum')
    ).reset_index()
    garden_analysis = garden_analysis[garden_analysis['total_volume'] > 0]
    if garden_analysis.empty: return []
    garden_analysis['avg_price'] = garden_analysis['total_value'] / garden_analysis['total_volume']
    top_gardens = garden_analysis.sort_values(by='avg_price', ascending=False).head(10)
    top_gardens = top_gardens.copy()
    top_gardens['total_volume'] = top_gardens['total_volume'].map('{:,.0f}'.format)
    top_gardens['avg_price'] = top_gardens['avg_price'].map('${:.2f}'.format)

    # Rename columns for display consistency
    top_gardens = top_gardens.rename(columns={'mark': 'Garden', 'total_volume': 'Volume (kg)', 'avg_price': 'Avg Price'})
    return top_gardens[['Garden', 'Volume (kg)', 'Avg Price']].to_dict(orient='records')

# =============================================================================
# Main Processing Loop
# =============================================================================

def main():
    logging.info("Starting Mombasa Data Analysis (JSON Generation Mode)...")

    if not os.path.exists(DATA_OUTPUT_DIR): os.makedirs(DATA_OUTPUT_DIR)

    conn = connect_db()
    sales_df_raw, offers_df_raw = fetch_data(conn)
    sales_df_all = prepare_sales_data(sales_df_raw)

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

        sales_week_raw = sales_df_raw[sales_df_raw['sale_number'] == week_number]
        offers_week = offers_df_raw[offers_df_raw['sale_number'] == week_number]
        sales_week = prepare_sales_data(sales_week_raw)

        # Determine Date and Year
        week_date = "Unknown"; year = "Unknown"
        if not sales_week_raw.empty: week_date = sales_week_raw['sale_date'].iloc[0]
        elif not offers_week.empty: week_date = offers_week['sale_date'].iloc[0]

        if week_date != "Unknown":
            try: year = pd.to_datetime(week_date).year
            except: pass
            
        # Extract just the sale week number (e.g., '35' from '2025-35')
        try:
            # Format as integer to remove leading zeros (e.g. 09 -> 9) for clean display
            sale_num_only = int(week_number.split('-')[1])
        except (IndexError, ValueError):
            sale_num_only = week_number


        # Run Analysis (Combined function)
        kpis, forecast_tables = analyze_kpis_and_forecast(sales_week, sales_df_all, sales_week_raw, offers_week)

        # Generate Charts and Tables
        charts = {
            'buyers': create_buyer_chart(sales_week),
            'grades': create_grade_chart(sales_week),
        }
        tables = {
            'gardens': create_garden_table_data(sales_week),
            'sell_through': forecast_tables['sell_through'],
            'realization': forecast_tables['realization']
        }

        # Structure the report data
        report_data = {
            'metadata': {
                'sale_number': week_number, 'sale_date': week_date, 'location': 'Mombasa',
                'year': year, 'sale_num_only': sale_num_only, 'generated_at': datetime.datetime.now().isoformat()
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

            # Add enhanced details to index
            report_index.append({
                'sale_number': week_number,
                'sale_num_only': sale_num_only,
                'sale_date': week_date,
                'year': year,
                'filename': filename,
                'location': 'Mombasa',
                'snapshot': kpis.get('SNAPSHOT', 'Awaiting Data.')
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