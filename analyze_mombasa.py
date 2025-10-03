import sqlite3
import pandas as pd
import logging
import os
import sys
import datetime
import altair as alt
import json
import numpy as np

# Configuration
DB_FILE = "market_reports.db"
DATA_OUTPUT_DIR = "report_data"
INDEX_FILE = os.path.join(DATA_OUTPUT_DIR, "mombasa_index.json")

# Configure logging
logging.basicConfig(level=logging.INFO, format='ANALYZER: %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}
# CRITICAL: Disable max rows for Altair to ensure all data is embedded in the JSON for interactivity
alt.data_transformers.disable_max_rows()

# =============================================================================
# Helper Functions (Database and Cleaning)
# (These remain the same as the previous robust versions)
# =============================================================================

def connect_db():
    if not os.path.exists(DB_FILE):
        logging.error(f"Database file not found: {DB_FILE}. Ensure scrapers ran successfully.");
        sys.exit(1)
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
        # Check if essential tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auction_sales';")
        if cursor.fetchone() is None:
            logging.warning("auction_sales table not found. Returning empty dataframes.")
            return pd.DataFrame(), pd.DataFrame()

        sales_df = pd.read_sql_query("SELECT * FROM auction_sales", conn)

        try:
            offers_df = pd.read_sql_query("SELECT * FROM auction_offers", conn)
        except pd.errors.DatabaseError:
            logging.warning("auction_offers table not found or readable.")
            offers_df = pd.DataFrame()

        # Numeric conversion
        for df, cols in [(sales_df, ['price', 'quantity_kgs']), (offers_df, ['valuation_or_rp', 'quantity_kgs'])]:
            for col in cols:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

        # Text cleaning
        text_cols_common = ['mark', 'grade', 'broker', 'lot_number', 'sale_number', 'sale_date']
        for df in [sales_df, offers_df]:
            for col in text_cols_common: df = clean_text_column(df, col)

        if 'buyer' in sales_df.columns:
             sales_df = clean_text_column(sales_df, 'buyer')

        # Minimal filtering (Keys only)
        keys = ['broker', 'lot_number', 'sale_number', 'sale_date']
        if not sales_df.empty and all(k in sales_df.columns for k in keys):
            sales_df = sales_df.dropna(subset=keys)
            sales_df = sales_df[sales_df['sale_number'] != 'UNKNOWN']

        if not offers_df.empty and all(k in offers_df.columns for k in keys):
            offers_df = offers_df.dropna(subset=keys)
            offers_df = offers_df[offers_df['sale_number'] != 'UNKNOWN']

        return sales_df, offers_df
    except Exception as e:
        logging.error(f"Error fetching data: {e}", exc_info=True); sys.exit(1)

def prepare_sales_data(sales_df_raw):
    # Ensure required columns for interactive charts and tables are present
    required_cols = ['quantity_kgs', 'price', 'mark', 'grade', 'buyer', 'broker', 'lot_number']

    if sales_df_raw.empty or not all(col in sales_df_raw.columns for col in required_cols):
         return pd.DataFrame()

    sales_df = sales_df_raw.dropna(subset=required_cols).copy()

    if 'quantity_kgs' in sales_df.columns and 'price' in sales_df.columns:
        sales_df = sales_df[(sales_df['quantity_kgs'] > 0) & (sales_df['price'] > 0)]

    if sales_df.empty: return pd.DataFrame()

    sales_df['value_usd'] = sales_df['price'] * sales_df['quantity_kgs']
    return sales_df

# =============================================================================
# Analysis Functions (KPIs, Forecast, and Snapshot)
# (These remain the same)
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
                    if change > 0.5: kpis['PRICE_CHANGE_CLASS'] = 'positive'
                    elif change < -0.5: kpis['PRICE_CHANGE_CLASS'] = 'negative'
                    else: kpis['PRICE_CHANGE_CLASS'] = 'neutral'

        if 'PRICE_CHANGE' not in kpis:
            kpis['PRICE_CHANGE'] = "N/A (First Sale)"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'; kpis['PRICE_CHANGE_NUMERIC'] = 0

    # 2. Forecast Analysis (Sell-Through)
    offers_calc = offers_df_week.copy(); sales_calc = sales_df_week_raw.copy()
    lots_offered = 0; lots_sold = 0

    if not offers_calc.empty and 'broker' in offers_calc.columns and 'lot_number' in offers_calc.columns:
        offers_calc['lot_key'] = offers_calc['broker'].astype(str) + '_' + offers_calc['lot_number'].astype(str)
        lots_offered = offers_calc['lot_key'].nunique()

    if not sales_calc.empty and 'broker' in sales_calc.columns and 'lot_number' in sales_calc.columns:
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
    if not offers_df_week.empty and not sales_df_week_raw.empty and 'valuation_or_rp' in offers_df_week.columns and 'price' in sales_df_week_raw.columns:
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
# NEW: Interactive Chart and Rich Data Generation
# =============================================================================

def create_interactive_dashboard(sales_df_week):
    """Creates a combined, interactive dashboard specification (Cross-filtering)."""
    if sales_df_week.empty:
        return {}

    # Define standard chart height for consistency
    CHART_HEIGHT = 350
    # Use a professional blue color consistent with the theme
    PRIMARY_COLOR = "#4285F4" # Google Blue

    # 1. Define Interactive Selections
    # Brush for continuous data (like price range)
    brush = alt.selection_interval(encodings=['x'])
    # Click selection for categorical data (like clicking a grade or broker in the legend)
    click_selection = alt.selection_multi(fields=['grade', 'broker'], bind='legend')

    # 2. Price Distribution (Horizontal Histogram)
    # This chart drives the filtering when the user drags a selection
    price_dist = alt.Chart(sales_df_week).mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('price:Q', bin=alt.Bin(maxbins=30), title='Price (USD/kg)'),
        y=alt.Y('count():Q', title='Number of Lots'),
        tooltip=[alt.Tooltip('price:Q', bin=True), alt.Tooltip('count():Q')]
    ).properties(
        title="Price Distribution (Click and drag to filter other charts)",
        height=CHART_HEIGHT,
        width='container' # Responsive width
    ).add_selection(
        brush
    )

    # 3. Grade Performance
    # This chart is filtered by the brush AND can filter others via the legend
    grade_performance = alt.Chart(sales_df_week).mark_bar().encode(
        x=alt.X('grade:N', title='Grade', sort='-y'),
        y=alt.Y('mean(price):Q', title='Average Price (USD/kg)'),
        # Color changes when selected in the legend (cross-filtering)
        color=alt.condition(click_selection, 'grade:N', alt.value('lightgray'), legend=alt.Legend(title="Filter by Grade/Broker")),
        tooltip=[alt.Tooltip('grade:N'), alt.Tooltip('mean(price):Q', format='$.2f'), alt.Tooltip('sum(quantity_kgs):Q', format=',.0f')]
    ).transform_filter(
        brush # Apply filter from the price distribution chart
    ).properties(
        title="Average Price by Grade",
        height=CHART_HEIGHT,
         width='container'
    ).add_selection(
        click_selection
    )

    # 4. Broker Performance
    # This chart is filtered by the brush AND can filter others via the legend
    broker_performance = alt.Chart(sales_df_week).mark_bar().encode(
         x=alt.X('broker:N', title='Broker', sort='-y'),
        y=alt.Y('sum(value_usd):Q', title='Total Value (USD)'),
        color=alt.condition(click_selection, 'broker:N', alt.value('lightgray')),
        tooltip=[alt.Tooltip('broker:N'), alt.Tooltip('sum(value_usd):Q', format='$,.0f'), alt.Tooltip('mean(price):Q', format='$.2f')]
    ).transform_filter(
        brush # Apply filter from the price distribution chart
    ).properties(
        title="Total Value by Broker",
        height=CHART_HEIGHT,
        width='container'
    )

    # 5. Combine the charts into a layout
    # Vertical concatenation (vconcat) and horizontal concatenation (hconcat)
    dashboard = alt.vconcat(
        price_dist,
        alt.hconcat(grade_performance, broker_performance, spacing=30),
        spacing=30
    ).resolve_scale(
        color='independent' # Ensure legends combine correctly
    ).configure_view(
        stroke=None # Clean look for the container
    )

    return dashboard.to_dict()

def create_buyer_chart(sales_df_week):
    """Generates a chart for the Top 15 Buyers."""
    CHART_HEIGHT = 350
    if sales_df_week.empty or 'buyer' not in sales_df_week.columns or 'value_usd' not in sales_df_week.columns: return {}

    # Aggregate data: Calculate total value and volume for each buyer
    buyer_agg = sales_df_week.groupby('buyer').agg(
        total_value=('value_usd', 'sum'),
        total_volume=('quantity_kgs', 'sum')
    ).reset_index()
    
    # Calculate average price
    buyer_agg['avg_price'] = buyer_agg.apply(lambda row: row['total_value'] / row['total_volume'] if row['total_volume'] > 0 else 0, axis=1)

    # Get Top 15
    top_buyers = buyer_agg.nlargest(15, 'total_value')

    if top_buyers.empty: return {}

    chart = alt.Chart(top_buyers).mark_bar().encode(
        y=alt.Y('buyer:N', title='Buyer', sort='-x'),
        x=alt.X('total_value:Q', title='Value (USD)'),
        # Use a professional color scale (e.g., 'blues')
        color=alt.Color('total_value:Q', scale=alt.Scale(scheme='blues'), legend=None),
        tooltip=[
            alt.Tooltip('buyer:N'),
            alt.Tooltip('total_value:Q', format='$,.0f', title='Value (USD)'),
            alt.Tooltip('total_volume:Q', format=',.0f', title='Volume (kg)'),
            alt.Tooltip('avg_price:Q', format='$.2f', title='Avg Price')
        ]
    ).properties(
        title="Top 15 Buyers (by Value)",
        height=CHART_HEIGHT,
        width='container' # Responsive width
    )
    return chart.to_dict()


def generate_raw_data_export(sales_df_week):
    """Prepares the full raw sales data for the interactive Tabulator table."""
    if sales_df_week.empty:
        return []

    # Select and rename columns for the front-end table
    export_df = sales_df_week[['mark', 'grade', 'lot_number', 'quantity_kgs', 'price', 'buyer', 'broker']].copy()
    export_df = export_df.rename(columns={
        'mark': 'Mark',
        'grade': 'Grade',
        'lot_number': 'Lot',
        'quantity_kgs': 'KGs',
        'price': 'Price (USD)',
        'buyer': 'Buyer',
        'broker': 'Broker'
    })
    
    # Ensure Lot number is treated as string for consistency
    export_df['Lot'] = export_df['Lot'].astype(str)

    # Keep data as numbers (int/float) for correct sorting in the frontend table
    # Replace any NaNs resulting from processing with None for JSON compatibility
    export_df = export_df.replace({np.nan: None})
    return export_df.to_dict(orient='records')


# =============================================================================
# Main Processing Loop
# =============================================================================

def main():
    logging.info("Starting Mombasa Data Analysis (Enhanced Interactive Mode)...")

    if not os.path.exists(DATA_OUTPUT_DIR): os.makedirs(DATA_OUTPUT_DIR)

    conn = connect_db()
    sales_df_raw, offers_df_raw = fetch_data(conn)
    sales_df_all = prepare_sales_data(sales_df_raw)

    # Determine unique weeks
    all_weeks = []
    if 'sale_number' in sales_df_raw.columns and not sales_df_raw.empty:
        all_weeks.extend(sales_df_raw['sale_number'].dropna().unique())
    if 'sale_number' in offers_df_raw.columns and not offers_df_raw.empty:
        all_weeks.extend(offers_df_raw['sale_number'].dropna().unique())

    all_weeks = sorted(list(set(all_weeks)))

    if len(all_weeks) == 0:
        logging.info("No sale data found in database. Exiting."); return

    report_index = []

    # Process each week individually
    for week_number in all_weeks:
        logging.info(f"Processing Sale: {week_number}")

        sales_week_raw = sales_df_raw[sales_df_raw['sale_number'] == week_number] if not sales_df_raw.empty else pd.DataFrame()
        offers_week = offers_df_raw[offers_df_raw['sale_number'] == week_number] if not offers_df_raw.empty else pd.DataFrame()
        sales_week = prepare_sales_data(sales_week_raw)

        # Determine Date and Year
        week_date = "Unknown"; year = "Unknown"
        if not sales_week_raw.empty and 'sale_date' in sales_week_raw.columns:
            week_date = sales_week_raw['sale_date'].iloc[0]
        elif not offers_week.empty and 'sale_date' in offers_week.columns:
            week_date = offers_week['sale_date'].iloc[0]

        if week_date != "Unknown":
            try: year = pd.to_datetime(week_date).year
            except: pass

        # Extract simple week number
        try:
            sale_num_only = int(str(week_number).split('-')[1])
        except (IndexError, ValueError):
            sale_num_only = week_number

        # Run Analysis (KPIs and Forecast)
        kpis, forecast_tables = analyze_kpis_and_forecast(sales_week, sales_df_all, sales_week_raw, offers_week)

        # Generate Charts and Rich Data (Enhanced)
        charts = {
            'interactive_dashboard': create_interactive_dashboard(sales_week),
            'buyers': create_buyer_chart(sales_week),
        }
        tables = {
            'sell_through': forecast_tables['sell_through'],
            'realization': forecast_tables['realization'],
            # NEW: Include raw data for the interactive table
            'raw_sales_data': generate_raw_data_export(sales_week)
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
        filename = f"mombasa_{str(week_number).replace('-', '_')}.json"
        filepath = os.path.join(DATA_OUTPUT_DIR, filename)

        try:
            with open(filepath, 'w') as f:
                # Use default=str as a safety catch for any non-standard types during serialization
                json.dump(report_data, f, indent=2, default=str)

            # Add details to index
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
        report_index.sort(key=lambda x: str(x['sale_number']), reverse=True)
        with open(INDEX_FILE, 'w') as f:
            json.dump(report_index, f, indent=2)
        logging.info(f"Generated index file: {INDEX_FILE} with {len(report_index)} entries.")
    except Exception as e:
        logging.error(f"Error saving index file: {e}")

    conn.close()
    logging.info("Analysis Complete.")

if __name__ == "__main__":
    main()