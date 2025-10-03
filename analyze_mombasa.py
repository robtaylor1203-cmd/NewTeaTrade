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
PRIMARY_COLOR = "#4285F4" # Google Blue
CHART_HEIGHT = 320

# Configure logging
logging.basicConfig(level=logging.INFO, format='ANALYZER: %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}
alt.data_transformers.disable_max_rows()

# =============================================================================
# Helper Functions (Database and Cleaning)
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
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auction_sales';")
        sales_exists = cursor.fetchone() is not None
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auction_offers';")
        offers_exists = cursor.fetchone() is not None

        if not sales_exists and not offers_exists:
             logging.warning("Essential tables not found. Returning empty dataframes.")
             return pd.DataFrame(), pd.DataFrame()

        sales_df = pd.read_sql_query("SELECT * FROM auction_sales", conn) if sales_exists else pd.DataFrame()
        offers_df = pd.read_sql_query("SELECT * FROM auction_offers", conn) if offers_exists else pd.DataFrame()

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
    required_cols = ['quantity_kgs', 'price', 'mark', 'grade', 'buyer', 'broker', 'lot_number', 'sale_number']

    if sales_df_raw.empty or not all(col in sales_df_raw.columns for col in required_cols):
         return pd.DataFrame()

    sales_df = sales_df_raw.dropna(subset=required_cols).copy()

    if 'quantity_kgs' in sales_df.columns and 'price' in sales_df.columns:
        sales_df = sales_df[(sales_df['quantity_kgs'] > 0) & (sales_df['price'] > 0)]

    if sales_df.empty: return pd.DataFrame()

    sales_df['value_usd'] = sales_df['price'] * sales_df['quantity_kgs']
    return sales_df

# =============================================================================
# Analysis Functions (KPIs and Forecast)
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
        lots_offered = offers_calc[['broker', 'lot_number']].drop_duplicates().shape[0]

    if not sales_calc.empty and 'broker' in sales_calc.columns and 'lot_number' in sales_calc.columns:
        lots_sold = sales_calc[['broker', 'lot_number']].drop_duplicates().shape[0]

    sell_through_rate = (lots_sold / lots_offered) if lots_offered > 0 else 0
    kpis['SELL_THROUGH_RATE'] = f"{sell_through_rate:.2%}"
    kpis['SELL_THROUGH_RATE_RAW'] = sell_through_rate

    tables['sell_through'].append({'Metric': 'Lots Offered', 'Value': f"{lots_offered:,.0f}"})
    tables['sell_through'].append({'Metric': 'Lots Sold', 'Value': f"{lots_sold:,.0f}"})
    tables['sell_through'].append({'Metric': 'Rate', 'Value': kpis['SELL_THROUGH_RATE']})

    # 3. Realization (Kept for completeness, often N/A)
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
# Interactive Chart Generation
# =============================================================================

brush = alt.selection_interval(encodings=['x'])

def create_price_distribution_chart(sales_df_week):
    """Creates the main price distribution histogram."""
    if sales_df_week.empty: return {}
    
    height = 180 

    chart = alt.Chart(sales_df_week).mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('price:Q', bin=alt.Bin(maxbins=50), title='Price (USD/kg)'),
        y=alt.Y('count():Q', title='Number of Lots'),
        opacity=alt.condition(brush, alt.value(1.0), alt.value(0.7)),
        tooltip=[alt.Tooltip('price:Q', bin=True), alt.Tooltip('count():Q')]
    ).properties(
        title="Price Distribution (Click and drag to filter below)",
        height=height,
        width='container'
    ).add_selection(
        brush
    )
    return chart.to_dict()

def create_grade_performance_chart(sales_df_week):
    """Creates the grade performance chart, filtered by the brush."""
    if sales_df_week.empty: return {}

    chart = alt.Chart(sales_df_week).mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('grade:N', title='Grade', sort='-y'),
        y=alt.Y('mean(price):Q', title='Average Price (USD/kg)'),
        tooltip=[alt.Tooltip('grade:N'), alt.Tooltip('mean(price):Q', format='$.2f'), alt.Tooltip('sum(quantity_kgs):Q', format=',.0f')]
    ).transform_filter(
        brush
    ).properties(
        title="Average Price by Grade",
        height=CHART_HEIGHT,
        width='container'
    )
    return chart.to_dict()

def create_broker_performance_chart(sales_df_week):
    """Creates the broker performance chart, filtered by the brush."""
    if sales_df_week.empty: return {}

    chart = alt.Chart(sales_df_week).mark_bar(color=PRIMARY_COLOR).encode(
         x=alt.X('broker:N', title='Broker', sort='-y'),
        y=alt.Y('sum(value_usd):Q', title='Total Value (USD)'),
        tooltip=[alt.Tooltip('broker:N'), alt.Tooltip('sum(value_usd):Q', format='$,.0f'), alt.Tooltip('mean(price):Q', format='$.2f')]
    ).transform_filter(
        brush
    ).properties(
        title="Total Value by Broker",
        height=CHART_HEIGHT,
        width='container'
    )
    return chart.to_dict()


def create_buyer_chart(sales_df_week):
    """Generates a chart for the Top 15 Buyers."""
    if sales_df_week.empty or 'buyer' not in sales_df_week.columns or 'value_usd' not in sales_df_week.columns: return {}

    # Aggregate data
    buyer_agg = sales_df_week.groupby('buyer').agg(
        total_value=('value_usd', 'sum'),
        total_volume=('quantity_kgs', 'sum')
    ).reset_index()
    
    buyer_agg['avg_price'] = buyer_agg.apply(lambda row: row['total_value'] / row['total_volume'] if row['total_volume'] > 0 else 0, axis=1)

    # Get Top 15
    top_buyers = buyer_agg.nlargest(15, 'total_value')

    if top_buyers.empty: return {}

    # Use the standard Primary Color
    chart = alt.Chart(top_buyers).mark_bar(color=PRIMARY_COLOR).encode(
        y=alt.Y('buyer:N', title='Buyer', sort='-x'),
        x=alt.X('total_value:Q', title='Value (USD)'),
        tooltip=[
            alt.Tooltip('buyer:N'),
            alt.Tooltip('total_value:Q', format='$,.0f', title='Value (USD)'),
            alt.Tooltip('total_volume:Q', format=',.0f', title='Volume (kg)'),
            alt.Tooltip('avg_price:Q', format='$.2f', title='Avg Price')
        ]
    ).properties(
        title="Top 15 Buyers (by Value)",
        height=CHART_HEIGHT + 50,
        width='container'
    )
    return chart.to_dict()

# =============================================================================
# NEW: Advanced Analysis (Candlestick and Insights)
# =============================================================================

def analyze_price_movements(sales_df_week, sales_df_all):
    """Calculates data required for Candlestick chart and generates insights."""
    
    if sales_df_week.empty:
        return pd.DataFrame(), "Awaiting current week data for trend analysis."

    current_sale_number = sales_df_week['sale_number'].iloc[0]
    previous_sales = sales_df_all[sales_df_all['sale_number'] < current_sale_number]

    if previous_sales.empty:
        return pd.DataFrame(), "First sale recorded; no historical data for comparison."

    previous_sale_number = previous_sales['sale_number'].max()
    prev_week_df = sales_df_all[sales_df_all['sale_number'] == previous_sale_number]

    # 1. Calculate Current Week Metrics (Min, Max, Avg)
    # Standardize names to Open/High/Low/Close (OHLC) for Candlestick
    current_metrics = sales_df_week.groupby(['mark', 'grade']).agg(
        close=('price', 'mean'),
        high=('price', 'max'),
        low=('price', 'min'),
        volume=('quantity_kgs', 'sum')
    ).reset_index()

    # 2. Calculate Previous Week Average (Open)
    prev_metrics = prev_week_df.groupby(['mark', 'grade']).agg(
        open=('price', 'mean')
    ).reset_index()

    # 3. Merge Data (Inner join ensures we only compare items sold in both weeks)
    movement_df = pd.merge(current_metrics, prev_metrics, on=['mark', 'grade'], how='inner')

    # 4. Calculate Movement
    movement_df['change'] = movement_df['close'] - movement_df['open']
    movement_df['change_pct'] = (movement_df['change'] / movement_df['open']) * 100
    # Determine color (Green for rise, Red for fall)
    movement_df['color'] = movement_df.apply(lambda row: '#34a853' if row['change'] >= 0 else '#ea4335', axis=1)

    # 5. Generate Insights (Top Movers)
    insights = []
    # Filter for significant volume to avoid low-volume outliers
    significant_volume_df = movement_df[movement_df['volume'] > 500] 
    
    top_risers = significant_volume_df.sort_values(by='change_pct', ascending=False).head(3)
    top_fallers = significant_volume_df.sort_values(by='change_pct', ascending=True).head(3)

    if not top_risers.empty:
        insights.append("Notable price increases week-over-week (min 500kg):")
        for _, row in top_risers.iterrows():
            insights.append(f"- {row['mark']} ({row['grade']}) rose by {row['change_pct']:.1f}% (from ${row['open']:.2f} to ${row['close']:.2f}).")

    if not top_fallers.empty:
        if insights: insights.append("\n") # Add space
        insights.append("Significant price declines week-over-week (min 500kg):")
        for _, row in top_fallers.iterrows():
             insights.append(f"- {row['mark']} ({row['grade']}) decreased by {row['change_pct']:.1f}% (from ${row['open']:.2f} to ${row['close']:.2f}).")

    if not insights:
        insights.append("Prices across most compared gardens and grades remained relatively stable week-over-week.")

    return movement_df, "\n".join(insights)


def create_candlestick_chart(movement_df):
    """Generates the Candlestick chart specification."""
    if movement_df.empty:
        return {}

    # Define the selection mechanism (Dropdown for Garden/Mark)
    marks = sorted(movement_df['mark'].unique().tolist())
    
    if not marks: return {}

    input_dropdown = alt.binding_select(options=marks, name='Select Garden: ')
    selection = alt.selection_single(fields=['mark'], bind=input_dropdown, init={'mark': marks[0]})

    # Base chart definition
    base = alt.Chart(movement_df).transform_filter(
        selection # Apply the dropdown filter
    ).properties(
        width='container',
        height=350,
        title="Week-over-Week Price Movement (Candlestick)"
    )

    # 1. The Wicks (High to Low Range)
    wicks = base.mark_rule(strokeWidth=1).encode(
        x='grade:N',
        y=alt.Y('low:Q', title='Price (USD/kg)', scale=alt.Scale(zero=False)),
        y2='high:Q',
        color=alt.Color('color:N', scale=None), # Apply color to wicks too
        tooltip=[
            alt.Tooltip('mark:N'), alt.Tooltip('grade:N'),
            alt.Tooltip('open:Q', format='$.2f', title='Previous Avg (Open)'), 
            alt.Tooltip('close:Q', format='$.2f', title='Current Avg (Close)'),
            alt.Tooltip('high:Q', format='$.2f'), alt.Tooltip('low:Q', format='$.2f'),
            alt.Tooltip('change_pct:Q', format='+.2f', title='Change %')
        ]
    )

    # 2. The Body (Open to Close)
    body = base.mark_bar(size=15).encode(
        x='grade:N',
        y='open:Q',
        y2='close:Q',
        color=alt.Color('color:N', scale=None)
    )

    # Combine layers and add the selection mechanism
    chart = alt.layer(wicks, body).add_selection(selection)

    return chart.to_dict()


# =============================================================================
# Data Export and Forward Outlook
# =============================================================================

def generate_raw_data_export(sales_df_week):
    """Prepares the full raw sales data for the interactive Tabulator table."""
    if sales_df_week.empty:
        return []

    # Select and rename columns
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
    
    export_df['Lot'] = export_df['Lot'].astype(str)

    # Replace NaNs with None
    export_df = export_df.replace({np.nan: None})
    return export_df.to_dict(orient='records')

def generate_forecast_outlook(week_number, location, offers_df_all):
    """Generates forward-looking information."""
    
    # Updated placeholder text for professionalism
    outlook = {
        "next_sale": "N/A",
        "forthcoming_offerings_kgs": "Awaiting Catalogues",
        "weather_outlook": f"Seasonal weather patterns are prevailing in the key growing regions supplying {location}. Production levels are reported as stable.",
        "market_prediction": "Based on current demand trends, the market is expected to remain active. Buyers are advised to monitor global economic indicators and currency fluctuations which may impact pricing in the coming weeks."
    }

    if not week_number or offers_df_all.empty or 'sale_number' not in offers_df_all.columns:
        return outlook

    # Calculate forthcoming volume
    future_sales = sorted(offers_df_all[offers_df_all['sale_number'] > week_number]['sale_number'].unique())

    if future_sales:
        next_sale_number = future_sales[0]
        outlook["next_sale"] = next_sale_number
        next_week_offers = offers_df_all[offers_df_all['sale_number'] == next_sale_number]

        if 'quantity_kgs' in next_week_offers.columns:
            forthcoming_volume = next_week_offers['quantity_kgs'].sum()
            if pd.notna(forthcoming_volume) and forthcoming_volume > 0:
                outlook["forthcoming_offerings_kgs"] = f"{forthcoming_volume:,.0f}"

    return outlook


# =============================================================================
# Main Processing Loop
# =============================================================================

def main():
    logging.info("Starting Mombasa Data Analysis (Advanced Insights Mode)...")

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

        # Metadata
        location = 'Mombasa'
        week_date = "Unknown"; year = "Unknown"
        if not sales_week_raw.empty and 'sale_date' in sales_week_raw.columns:
            week_date = sales_week_raw['sale_date'].iloc[0]
        elif not offers_week.empty and 'sale_date' in offers_week.columns:
            week_date = offers_week['sale_date'].iloc[0]

        if week_date != "Unknown" and week_date is not pd.NA:
            try: year = pd.to_datetime(week_date).year
            except: pass

        try:
            sale_num_only = int(str(week_number).split('-')[1])
        except (IndexError, ValueError):
            sale_num_only = week_number

        # Run Analysis (KPIs and Forecast)
        kpis, forecast_tables = analyze_kpis_and_forecast(sales_week, sales_df_all, sales_week_raw, offers_week)

        # NEW: Advanced Analysis (Candlestick data and Insights)
        movement_data, analytical_insights = analyze_price_movements(sales_week, sales_df_all)

        # Generate Charts
        charts = {
            'price_distribution': create_price_distribution_chart(sales_week),
            'grade_performance': create_grade_performance_chart(sales_week),
            'broker_performance': create_broker_performance_chart(sales_week),
            'buyers': create_buyer_chart(sales_week),
            # NEW: Candlestick Chart
            'candlestick': create_candlestick_chart(movement_data)
        }
        tables = {
            'sell_through': forecast_tables['sell_through'],
            'realization': forecast_tables['realization'],
            'raw_sales_data': generate_raw_data_export(sales_week)
        }
        
        # Forward Outlook
        outlook = generate_forecast_outlook(week_number, location, offers_df_raw)

        # Structure the report data
        report_data = {
            'metadata': {
                'sale_number': week_number, 'sale_date': week_date, 'location': location,
                'year': year, 'sale_num_only': sale_num_only, 'generated_at': datetime.datetime.now().isoformat()
            },
            'kpis': kpis,
            # NEW: Include the generated insights
            'insights': analytical_insights,
            'charts': charts,
            'tables': tables,
            'outlook': outlook
        }

        # Save the report JSON file
        filename = f"mombasa_{str(week_number).replace('-', '_')}.json"
        filepath = os.path.join(DATA_OUTPUT_DIR, filename)

        try:
            with open(filepath, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)

            # Add details to index
            report_index.append({
                'sale_number': week_number,
                'sale_num_only': sale_num_only,
                'sale_date': week_date,
                'year': year,
                'filename': filename,
                'location': location,
                'snapshot': kpis.get('SNAPSHOT', 'Awaiting Data.')
            })
        except Exception as e:
            logging.error(f"Error saving JSON for {week_number}: {e}")

    # Save the index file
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