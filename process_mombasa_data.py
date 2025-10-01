import sqlite3
import pandas as pd
import os
import re
from datetime import datetime
import time
import logging
import warnings

# Imports for unstructured data processing
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
try:
    import docx
except ImportError:
    docx = None

# =============================================================================
# Configuration
# =============================================================================

DB_FILE = "market_reports.db"
MOMBASA_DIR = r"C:\Users\mikin\projects\NewTeaTrade\Mombasa"
SOURCE_LOCATION = "Mombasa"

warnings.filterwarnings("ignore", message="Cannot parse header or footer so it will be ignored")
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Define Data Types
DATA_TYPE_OFFER = 'OFFER'
DATA_TYPE_SALE = 'SALE'
DATA_TYPE_SUMMARY = 'SUMMARY'
DATA_TYPE_COMMENTARY = 'COMMENTARY'

# V5: Define the prioritized list for Mark (Garden) used in COALESCE strategy
MARK_ALIASES = ['Selling Mark', 'Garden', 'Mark', 'Estate', 'Factory', 'Selling Mark - MF Mark']

# Flexible Column Mapping: Headers listed in order of preference.
COLUMN_MAP_LOT_DETAILS = {
    'broker': ['Broker'],
    'mark': MARK_ALIASES, # Use the prioritized list
    'grade': ['Grade'],
    'lot_number': ['LotNo', 'Lot No', 'Lot', 'Lot.No'],
    'invoice_number': ['Invoice', 'Inv.No', 'Invoice No'],
    'quantity_kgs': ['Net Weight', 'Kilos', 'Kgs', 'Quantity (Kg)', 'Total Weight'],
    'package_count': ['Bags', 'Pkgs'],
    'price': ['Purchased Price', 'Final Price', 'Price', 'Price (USD)', 'Price (USc)'],
    'valuation_or_rp': ['Valuation', 'Asking Price', 'RP'],
    'buyer': ['Buyer', 'Buyer Name', 'Final Buyer'],
    # Fields used for internal metadata extraction
    'sale_date_internal': ['Selling End Time', 'Sale Date'], 
    'sale_number_internal': ['Sale Code', 'Auction'], 
}

COLUMN_MAP_GRADE_SUMMARY = {
    'grade': ['Region/Grade'],
    'lots': ['Lots'],
    'quantity_kgs': ['Kilos', 'Pkgs', 'Kgs'],
}

HEADER_KEYWORDS = ['LotNo', 'Garden', 'Grade', 'Invoice', 'Pkgs', 'Kilos', 'RP', 'Valuation']

# =============================================================================
# Database Initialization
# =============================================================================

def initialize_database():
    logging.info("Initializing database schema (Offers, Sales, Summaries, Commentary)...")
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Processing Log
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY, file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    records_inserted INTEGER, data_type TEXT NOT NULL, status TEXT NOT NULL,
                    UNIQUE(file_identifier, data_type)
                )
            """)
            # Auction Sales (Note: UNIQUE constraints are crucial for UPSERT)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auction_sales (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    broker TEXT, mark TEXT, grade TEXT, lot_number TEXT NOT NULL, invoice_number TEXT,
                    quantity_kgs REAL, package_count INTEGER, price REAL NOT NULL, buyer TEXT NOT NULL,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, lot_number, broker)
                )
            """)
            # Auction Offers
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auction_offers (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    broker TEXT, mark TEXT, grade TEXT, lot_number TEXT NOT NULL, invoice_number TEXT,
                    quantity_kgs REAL, package_count INTEGER, valuation_or_rp REAL,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, lot_number, broker)
                )
            """)
            # Grade Summary
            conn.execute("""
                 CREATE TABLE IF NOT EXISTS grade_summary (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    auction_type TEXT NOT NULL, grade TEXT NOT NULL, lots INTEGER, quantity_kgs REAL,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, auction_type, grade)
                )
            """)
            # V5: Market Commentary (Unstructured Data)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_commentary (
                    id INTEGER PRIMARY KEY,
                    source_location TEXT NOT NULL,
                    report_date TEXT,
                    sale_number TEXT,
                    content_type TEXT NOT NULL, -- e.g., WEATHER, MARKET_REPORT
                    content TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, content_type, source_file)
                )
            """)
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")

# =============================================================================
# Utility Functions (Logging, Mapping, Parsing)
# =============================================================================

def get_file_identifier(filename, sheetname=None):
    if sheetname:
        return f"{filename}::{sheetname}"
    return filename

def is_processed(file_identifier, conn, data_type):
    # V5: This function is kept for logging purposes but may not be used to skip processing
    # if UPSERT enrichment is required.
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM processing_log WHERE file_identifier = ? AND status = 'SUCCESS' AND data_type = ?", (file_identifier, data_type))
        return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logging.error(f"Database check error: {e}")
        return False

def log_processed(file_identifier, records_count, conn, data_type, status='SUCCESS'):
    try:
        timestamp = datetime.now().isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO processing_log 
            (file_identifier, processed_timestamp, records_inserted, data_type, status) 
            VALUES (?, ?, ?, ?, ?)
        """, (file_identifier, timestamp, records_count, data_type, status))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to log file processing: {e}")

def map_columns(df_columns, mapping_dict):
    """Maps Excel columns. V5: Modified to support COALESCE strategy for 'mark'."""
    mapping = {}
    normalized_df_cols = {str(col).strip().lower(): col for col in df_columns}
    priorities = {} 

    # V5: Track mapped 'mark' columns separately
    mapped_mark_cols = {} 

    for db_col, aliases in mapping_dict.items():
        for priority_index, alias in enumerate(aliases):
            normalized_alias = alias.strip().lower()
            if normalized_alias in normalized_df_cols:
                
                # V5: Handle 'mark' columns separately
                if db_col == 'mark':
                    # Store the original Excel column name and its priority index
                    mapped_mark_cols[normalized_df_cols[normalized_alias]] = priority_index
                    continue

                # Prioritization for non-mark columns
                prioritized_cols = ['price', 'quantity_kgs', 'package_count', 'valuation_or_rp', 'sale_date_internal', 'sale_number_internal']
                
                if db_col in prioritized_cols:
                    if db_col not in priorities or priority_index < priorities[db_col]:
                        if db_col in mapping.values():
                             previous_header = next(k for k, v in mapping.items() if v == db_col)
                             del mapping[previous_header]
                        
                        mapping[normalized_df_cols[normalized_alias]] = db_col
                        priorities[db_col] = priority_index
                
                elif db_col not in mapping.values():
                    mapping[normalized_df_cols[normalized_alias]] = db_col
                    
    return mapping, mapped_mark_cols

def find_header_row(xls_file, sheetname, keywords, max_scan_rows=20):
    try:
        df_preview = pd.read_excel(xls_file, sheet_name=sheetname, header=None, nrows=max_scan_rows)
        normalized_keywords = set(k.strip().lower() for k in keywords)
        
        for index, row in df_preview.iterrows():
            row_values = set(str(cell).strip().lower() for cell in row if pd.notna(cell))
            matches = normalized_keywords.intersection(row_values)
            
            if len(matches) >= min(3, len(keywords)):
                return index
    except Exception as e:
        logging.error(f"Error while trying to find header row in {sheetname}: {e}")
    return None

# (Date parsing functions parse_date, extract_sale_number_from_string, extract_metadata remain the same as V4)

def parse_date(date_str, year_hint=None):
    """Prioritizes DD/MM/YYYY and handles milliseconds."""
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%d")
    if pd.isna(date_str):
        return None
    date_str = str(date_str).strip()
    if re.match(r"^\d{6}$", date_str):
        try:
            date_obj = datetime.strptime(date_str, "%d%m%y")
            if year_hint:
                 date_obj = date_obj.replace(year=int(year_hint))
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass
    date_str_cleaned = re.sub(r'[:.]\d{3}$', '', date_str)
    formats = [
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", 
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str_cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def extract_sale_number_from_string(sn_str, sale_date_hint=None):
    if pd.isna(sn_str) or not sn_str:
        return "Unknown"
    sn_str = str(sn_str)
    if '/' in sn_str and re.match(r'\d{4}/\d{1,2}', sn_str):
         return sn_str.replace('/', '-')
    elif 'Sale' in sn_str:
         match = re.search(r"Sale (\d+)", sn_str)
         if match:
              sale_prefix = "UnknownYear"
              if sale_date_hint and isinstance(sale_date_hint, str) and '-' in sale_date_hint:
                  try:
                      datetime.strptime(sale_date_hint, "%Y-%m-%d")
                      sale_prefix = sale_date_hint.split('-')[0]
                  except (ValueError, IndexError):
                      pass
              return f"{sale_prefix}-{int(match.group(1)):02d}"
    return "Unknown"

def extract_metadata(filename, df=None):
    sale_number, sale_date, year_hint = None, None, None
    
    # 1. AuctionSummary/CompleteOfferLots Filename Pattern
    pattern1 = re.compile(r"(?:AuctionSummary_|CompleteOfferLots_)\[?(\d{4})-(\d{2})\]?_(\d{6})\.xlsx", re.IGNORECASE)
    match1 = pattern1.match(filename)
    if match1:
        year_hint, week, date_str = match1.groups()
        sale_number = f"{year_hint}-{week}"
        sale_date = parse_date(date_str, year_hint)

    # 2. Sale Catalogue Filename Pattern
    if not sale_number:
        pattern2 = re.compile(r"Sale (\d+)_Catalogue_(\d{2})_(\d{2})_(\d{4})", re.IGNORECASE)
        match2 = pattern2.search(filename)
        if match2:
            sale_num, day, month, year = match2.groups()
            sale_number = f"{year}-{int(sale_num):02d}"
            sale_date = parse_date(f"{day}/{month}/{year}")

    # 3. Fallback to internal data
    if df is not None and (not sale_number or not sale_date):
        internal_mapping, _ = map_columns(df.columns, COLUMN_MAP_LOT_DETAILS)
        df_mapped = df.rename(columns=internal_mapping)

        if not sale_date and 'sale_date_internal' in df_mapped.columns:
            date_val = df_mapped['sale_date_internal'].dropna().iloc[0] if not df_mapped['sale_date_internal'].dropna().empty else None
            if date_val:
                 sale_date = parse_date(date_val)

        if not sale_number and 'sale_number_internal' in df_mapped.columns:
             sn_val = df_mapped['sale_number_internal'].dropna().iloc[0] if not df_mapped['sale_number_internal'].dropna().empty else None
             if sn_val:
                sale_number = extract_sale_number_from_string(str(sn_val), sale_date)

    return sale_number or "Unknown", sale_date or "Unknown"

# =============================================================================
# Data Loading Functions
# =============================================================================

def execute_insert(conn, table_name, df):
    """V5: Handles database insertion using UPSERT (INSERT OR UPDATE) for data enrichment."""
    if df.empty:
        return 0
    
    sql = ""
    try:
        # Prepare data
        records = df.where(pd.notnull(df), None).to_records(index=False)
        records_list = [tuple(rec) for rec in records]

        cursor = conn.cursor()
        placeholders = ', '.join(['?'] * len(df.columns))
        columns_str = ', '.join(df.columns)
        
        # --- V5: UPSERT Logic ---
        
        if table_name == 'auction_offers':
            # Conflict target based on the unique constraint
            conflict_target = "(source_location, sale_number, lot_number, broker)"
            
            # Columns to update using enrichment strategy (COALESCE)
            # If the new data (excluded) is NOT NULL, use it; otherwise, keep the existing data.
            update_enrich_cols = [
                'valuation_or_rp', 'mark', 'quantity_kgs', 'package_count', 
                'invoice_number', 'grade', 'sale_date', 'broker'
            ]
            # Columns to always update with the latest info
            update_always_cols = ['source_file_identifier', 'processed_timestamp']

            update_statements = []
            for col in update_enrich_cols:
                if col in df.columns:
                    # COALESCE(excluded.col, col)
                    update_statements.append(f"{col} = COALESCE(excluded.{col}, {col})")
            
            for col in update_always_cols:
                 if col in df.columns:
                    update_statements.append(f"{col} = excluded.{col}")

            if not update_statements:
                 sql = f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            else:
                update_str = ", ".join(update_statements)
                sql = f"""
                    INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
                    ON CONFLICT{conflict_target}
                    DO UPDATE SET {update_str}
                """

        elif table_name == 'auction_sales':
            # For sales, we generally trust the latest report. If a conflict occurs, we overwrite.
            conflict_target = "(source_location, sale_number, lot_number, broker)"
            update_statements = [f"{col} = excluded.{col}" for col in df.columns]

            if not update_statements:
                 sql = f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            else:
                update_str = ", ".join(update_statements)
                sql = f"""
                    INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
                    ON CONFLICT{conflict_target}
                    DO UPDATE SET {update_str}
                """

        else:
            # Fallback for other tables (Summary, Commentary) - use IGNORE
            sql = f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Execute the command
        cursor.executemany(sql, records_list)
        conn.commit()
        
        # Returns rows affected (inserted or updated)
        return cursor.rowcount

    except sqlite3.Error as e:
        logging.error(f"Database insertion (UPSERT) error into {table_name}: {e}. SQL: {sql if sql else 'N/A'}")
        conn.rollback()
        return 0

def clean_numeric_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.replace(r'[$,]', '', regex=True).str.strip()
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    return df

def load_lot_details(df, metadata, data_type, conn, use_internal_metadata=False):
    """Cleans and loads lot data. V5: Implements COALESCE for 'mark' in pandas."""
    file_identifier = metadata['file_identifier']
    
    # Determine target table
    if data_type == DATA_TYPE_SALE:
        target_table = 'auction_sales'
        required_specific_cols = ['price', 'buyer']
        logging.info(f"  [PROCESSING SALE] {file_identifier}")
    elif data_type == DATA_TYPE_OFFER:
        target_table = 'auction_offers'
        required_specific_cols = []
        logging.info(f"  [PROCESSING OFFER] {file_identifier}")
    else:
        raise ValueError(f"Invalid data_type provided: {data_type}")

    try:
        # 1. Map Columns (V5: Returns standard mapping and specific mark mapping)
        column_mapping, mapped_mark_cols = map_columns(df.columns, COLUMN_MAP_LOT_DETAILS)
        
        # Apply standard renaming
        df = df.rename(columns=column_mapping)

        # V5: Define noise values for text cleaning
        noise_values = {'NAN', 'NONE', '', '-', 'NIL'}

        # 2. V5: Implement COALESCE strategy for 'mark' (Pandas level)
        if mapped_mark_cols:
            # Sort the mapped mark columns by priority (lower index is better)
            sorted_mark_cols = sorted(mapped_mark_cols, key=mapped_mark_cols.get)
            
            # Clean and normalize the text in these columns first
            for col in sorted_mark_cols:
                 df[col] = df[col].astype(str).str.strip().str.upper()
                 df[col] = df[col].replace(noise_values, None)

            # Apply COALESCE: Start with the highest priority column
            df['mark'] = df[sorted_mark_cols[0]]
            
            # Iteratively fill missing values with the next priority column
            for i in range(1, len(sorted_mark_cols)):
                df['mark'] = df['mark'].fillna(df[sorted_mark_cols[i]])
        
        # 3. Handle Metadata
        if use_internal_metadata:
            logging.info("    [INFO] Using internal metadata extraction (Multi-sale file).")
            if 'sale_date_internal' in df.columns or 'sale_number_internal' in df.columns:
                if 'sale_date_internal' in df.columns:
                    df['sale_date'] = df['sale_date_internal'].apply(lambda x: parse_date(x))
                
                if 'sale_number_internal' in df.columns:
                    df['sale_number'] = df.apply(lambda row: extract_sale_number_from_string(
                        str(row.get('sale_number_internal')), row.get('sale_date')
                    ), axis=1)

        # Fallback Metadata
        if 'sale_date' not in df.columns:
             df['sale_date'] = metadata['sale_date']
        else:
             df['sale_date'] = df['sale_date'].fillna(metadata['sale_date'])

        if 'sale_number' not in df.columns:
            df['sale_number'] = metadata['sale_number']
        else:
            df['sale_number'] = df['sale_number'].fillna(metadata['sale_number'])

        # Filter invalid metadata rows
        df = df[(df['sale_number'] != 'Unknown') & (df['sale_date'] != 'Unknown') & df['sale_date'].notna() & df['sale_number'].notna()]

        if df.empty:
            log_processed(file_identifier, 0, conn, data_type, status='SUCCESS_NO_DATA')
            return

        # 4. Clean Data (Numeric)
        numeric_cols = ['price', 'quantity_kgs', 'valuation_or_rp', 'package_count']
        for col in numeric_cols:
            df = clean_numeric_column(df, col)
        
        if 'package_count' in df.columns:
             df['package_count'] = df['package_count'].round().astype('Int64') 

        # Clean Data (Text Identifiers - excluding 'mark' which is already handled)
        text_cols = ['grade', 'lot_number', 'broker', 'buyer', 'invoice_number']
        for col in text_cols:
             if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].replace(noise_values, None)

        # Add remaining metadata
        df['source_location'] = SOURCE_LOCATION
        df['source_file_identifier'] = file_identifier
        df['processed_timestamp'] = metadata['timestamp']

        # 5. Filter required columns
        # Broker and Lot Number are required for uniqueness. Mark and Grade are highly desired.
        required_base_cols = ['lot_number', 'broker', 'mark', 'grade'] 
        required_cols = required_base_cols + required_specific_cols

        if all(col in df.columns for col in required_cols):
             # Drop rows where required columns are None (crucial for Mark/Garden)
             df = df.dropna(subset=required_cols)
        else:
             missing = [col for col in required_cols if col not in df.columns]
             # Check if the essential keys (lot/broker) are missing
             if 'lot_number' not in df.columns or 'broker' not in df.columns:
                logging.warning(f"    Missing essential columns (Lot/Broker) for {data_type}: {missing}. Skipping load.")
                log_processed(file_identifier, 0, conn, data_type, status='FAILED_MISSING_COLS')
                return
             # If only mark/grade/etc are missing, drop those specific rows
             df = df.dropna(subset=required_cols)


        # 6. Load into Database (Now uses UPSERT via execute_insert)
        db_columns = [
            'source_location', 'sale_date', 'sale_number', 'broker', 'mark', 'grade', 
            'lot_number', 'invoice_number', 'quantity_kgs', 'package_count', 
            'source_file_identifier', 'processed_timestamp'
        ]
        if data_type == DATA_TYPE_SALE:
            db_columns.extend(['price', 'buyer'])
        elif data_type == DATA_TYPE_OFFER:
            db_columns.append('valuation_or_rp')

        data_to_insert = df[df.columns.intersection(db_columns)]
        
        # V5: execute_insert now handles UPSERT logic
        affected_count = execute_insert(conn, target_table, data_to_insert)
        
        if affected_count > 0:
            logging.info(f"    [SUCCESS] Inserted/Updated {affected_count} records in {target_table}.")
        else:
             logging.info(f"    [INFO] No changes detected in {target_table}.")

        log_processed(file_identifier, affected_count, conn, data_type, status='SUCCESS')

    except Exception as e:
        logging.error(f"  [ERROR] Unexpected error processing lots {file_identifier}: {e}", exc_info=True)
        log_processed(file_identifier, 0, conn, data_type, status='FAILED_PROCESSING')


def load_grade_summary(df, metadata, auction_type, conn):
    # (Logic remains similar, using standard INSERT OR IGNORE via execute_insert)
    file_identifier = metadata['file_identifier']
    data_type = DATA_TYPE_SUMMARY
    logging.info(f"  [PROCESSING SUMMARY] {file_identifier} (Type: {auction_type})")

    try:
        # 1. Map Columns
        column_mapping, _ = map_columns(df.columns, COLUMN_MAP_GRADE_SUMMARY)
        df = df.rename(columns=column_mapping)

        # 2. Clean Data
        df = clean_numeric_column(df, 'quantity_kgs')
        df = clean_numeric_column(df, 'lots')

        if 'lots' in df.columns:
            df['lots'] = df['lots'].round().astype('Int64')

        noise_values = {'NAN', 'NONE', '', '-', 'NIL'}
        if 'grade' in df.columns:
             df['grade'] = df['grade'].astype(str).str.strip().str.upper()
             df['grade'] = df['grade'].replace(noise_values, None)
        
        # 3. Metadata and Filter
        df['source_location'] = SOURCE_LOCATION
        df['sale_date'] = metadata['sale_date']
        df['sale_number'] = metadata['sale_number']
        df['auction_type'] = auction_type
        df['source_file_identifier'] = file_identifier
        df['processed_timestamp'] = metadata['timestamp']

        if 'grade' in df.columns:
             df = df.dropna(subset=['grade'])
             filter_keywords = "TOTAL|KENYA|BURUNDI|UGANDA|RWANDA|MALAWI|TANZANIA|MOZAMBIQUE|ETHIOPIA|DRC"
             df = df[~df['grade'].str.contains(filter_keywords, na=False)]
        else:
             log_processed(file_identifier, 0, conn, data_type, status='FAILED_MISSING_COLS')
             return

        # 4. Load
        db_columns = [
            'source_location', 'sale_date', 'sale_number', 'auction_type', 'grade', 
            'lots', 'quantity_kgs', 'source_file_identifier', 'processed_timestamp'
        ]
        data_to_insert = df[df.columns.intersection(db_columns)]
        inserted_count = execute_insert(conn, 'grade_summary', data_to_insert)
        
        if inserted_count > 0:
            logging.info(f"    [SUCCESS] Inserted {inserted_count} new summary records.")
        
        log_processed(file_identifier, inserted_count, conn, data_type, status='SUCCESS')

    except Exception as e:
        logging.error(f"  [ERROR] Unexpected error processing summary {file_identifier}: {e}", exc_info=True)
        log_processed(file_identifier, 0, conn, data_type, status='FAILED_PROCESSING')

# =============================================================================
# File Type Specific Processors (Handlers)
# =============================================================================

def process_auction_summary(filepath, filename, conn):
    logging.info(f"\n[HANDLER] AuctionSummary (Offers/Summary): {filename}")
    sheet_configs = {
        'Detail': {'header': 0, 'type': DATA_TYPE_OFFER},
        'Main Summary': {'header': 2, 'type': DATA_TYPE_SUMMARY, 'auction_type': 'Main'},
        'Secondary Summary': {'header': 2, 'type': DATA_TYPE_SUMMARY, 'auction_type': 'Secondary'},
    }
    try:
        xls_file = pd.ExcelFile(filepath, engine='openpyxl')
        sale_number, sale_date = extract_metadata(filename)
        
        for sheetname, config in sheet_configs.items():
            if sheetname in xls_file.sheet_names:
                data_type = config['type']
                file_identifier = get_file_identifier(filename, sheetname)
                
                # V5: We intentionally DO NOT check is_processed here for structured data.
                # We want the UPSERT logic to run every time to ensure data enrichment.

                df = pd.read_excel(xls_file, sheet_name=sheetname, header=config['header'])
                
                metadata = {'file_identifier': file_identifier, 'sale_number': sale_number, 'sale_date': sale_date, 'timestamp': datetime.now().isoformat()}

                if data_type in [DATA_TYPE_SALE, DATA_TYPE_OFFER]:
                    load_lot_details(df, metadata, data_type, conn, use_internal_metadata=False)
                elif data_type == DATA_TYPE_SUMMARY:
                    load_grade_summary(df, metadata, config['auction_type'], conn)
    except Exception as e:
        logging.error(f"  [ERROR] Failed to process {filename}: {e}")

def process_complete_offer_lots(filepath, filename, conn):
    logging.info(f"\n[HANDLER] CompleteOfferLots (Offers): {filename}")
    data_type = DATA_TYPE_OFFER
    try:
        xls_file = pd.ExcelFile(filepath, engine='openpyxl')
        sale_number, sale_date = extract_metadata(filename)

        for sheetname in xls_file.sheet_names:
            file_identifier = get_file_identifier(filename, sheetname)
            
            # V5: Intentionally allow re-processing for UPSERT.

            header_row = find_header_row(xls_file, sheetname, HEADER_KEYWORDS)
            if header_row is not None:
                logging.info(f"  [INFO] Found headers on row {header_row + 1} for sheet {sheetname}")
                df = pd.read_excel(xls_file, sheet_name=sheetname, header=header_row)
                df['Broker'] = sheetname
                metadata = {'file_identifier': file_identifier, 'sale_number': sale_number, 'sale_date': sale_date, 'timestamp': datetime.now().isoformat()}
                load_lot_details(df, metadata, data_type, conn, use_internal_metadata=False)
            else:
                logging.warning(f"  [WARNING] Could not find header row in sheet: {sheetname}.")
                log_processed(file_identifier, 0, conn, data_type, status='FAILED_DYNAMIC_HEADER')
    except Exception as e:
        logging.error(f"  [ERROR] Failed to process {filename}: {e}")

def process_standard_format(filepath, filename, conn, data_type, target_sheet=None, clean_second_row=False, use_internal_metadata=False):
    handler_name = "Sale Catalogue (Offers)" if data_type == DATA_TYPE_OFFER else "GeneralReport (Sales)"
    logging.info(f"\n[HANDLER] {handler_name}: {filename}")
    try:
        xls_file = pd.ExcelFile(filepath, engine='openpyxl')
        
        if target_sheet and target_sheet in xls_file.sheet_names:
            sheets_to_process = [target_sheet]
        elif not target_sheet and xls_file.sheet_names:
            sheets_to_process = [xls_file.sheet_names[0]]
        else:
            return

        first_sheet_name = sheets_to_process[0]
        df_initial = pd.read_excel(xls_file, sheet_name=first_sheet_name, header=0)
        sale_number, sale_date = extract_metadata(filename, df_initial)

        for sheetname in sheets_to_process:
            file_identifier = get_file_identifier(filename, sheetname)
            
            # V5: Intentionally allow re-processing for UPSERT.

            df = df_initial if sheetname == first_sheet_name else pd.read_excel(xls_file, sheet_name=sheetname, header=0)

            if clean_second_row and not df.empty:
                 if df.iloc[0].isnull().sum() > len(df.columns) / 2:
                    logging.info("  [INFO] Cleaning second row (noise/metadata).")
                    df = df.drop(0).reset_index(drop=True)
            
            metadata = {'file_identifier': file_identifier, 'sale_number': sale_number, 'sale_date': sale_date, 'timestamp': datetime.now().isoformat()}
            load_lot_details(df, metadata, data_type, conn, use_internal_metadata=use_internal_metadata)
    except Exception as e:
        logging.error(f"  [ERROR] Failed to process {filename}: {e}")

# =============================================================================
# V5: Unstructured Data Processor
# =============================================================================

def extract_text_from_file(filepath, extension):
    """Extracts raw text content from PDF, DOCX, or TXT files."""
    text = ""
    try:
        if extension == '.pdf':
            if fitz:
                doc = fitz.open(filepath)
                for page in doc:
                    text += page.get_text()
            else:
                logging.warning(f"PyMuPDF not installed. Skipping PDF: {os.path.basename(filepath)}")
                return None
        elif extension == '.docx':
            if docx:
                doc = docx.Document(filepath)
                for para in doc.paragraphs:
                    text += para.text + "\n"
            else:
                logging.warning(f"python-docx not installed. Skipping DOCX: {os.path.basename(filepath)}")
                return None
        elif extension == '.txt':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
    except Exception as e:
        logging.error(f"Error extracting text from {os.path.basename(filepath)}: {e}")
        return None
    return text.strip()

def process_unstructured_report(filepath, filename, conn):
    """Handler for PDF, DOCX, TXT reports."""
    logging.info(f"\n[HANDLER] Unstructured Report: {filename}")
    data_type = DATA_TYPE_COMMENTARY
    file_identifier = get_file_identifier(filename)

    # We only process unstructured data once unless the content changes (which we don't track here)
    if is_processed(file_identifier, conn, data_type):
        logging.info(f"  [SKIPPING] Already processed: {file_identifier}")
        return

    _, extension = os.path.splitext(filename.lower())
    content = extract_text_from_file(filepath, extension)

    if content:
        # Attempt to extract metadata (using filename patterns)
        sale_number, report_date = extract_metadata(filename)
        timestamp = datetime.now().isoformat()

        # Determine Content Type based on filename heuristics
        if 'weather' in filename.lower():
            content_type = 'WEATHER'
        elif 'market report' in filename.lower() or 'weekly report' in filename.lower():
            content_type = 'MARKET_REPORT'
        else:
            content_type = 'GENERAL'

        # Prepare data for insertion
        data = {
            'source_location': SOURCE_LOCATION,
            'report_date': report_date,
            'sale_number': sale_number,
            'content_type': content_type,
            'content': content,
            'source_file': filename,
            'processed_timestamp': timestamp
        }
        df = pd.DataFrame([data])

        # Insert into database (uses INSERT OR IGNORE via execute_insert)
        inserted_count = execute_insert(conn, 'market_commentary', df)
        
        if inserted_count > 0:
            logging.info(f"    [SUCCESS] Extracted content from {filename}.")
        
        log_processed(file_identifier, inserted_count, conn, data_type, status='SUCCESS')
    else:
        log_processed(file_identifier, 0, conn, data_type, status='FAILED_EXTRACTION')

# =============================================================================
# Main Processor
# =============================================================================

def run_processor():
    start_time = time.time()
    logging.info("--- Starting Mombasa Data Warehouse Processor V5 (Enrichment & Unstructured) ---")
    
    if not os.path.exists(MOMBASA_DIR):
        logging.error(f"Directory not found: {MOMBASA_DIR}")
        return

    initialize_database()
    
    try:
        with sqlite3.connect(DB_FILE) as conn:
            logging.info(f"Scanning directory: {MOMBASA_DIR}")
            
            try:
                # V5: Scan for structured and unstructured files
                all_files = [f for f in os.listdir(MOMBASA_DIR) if not f.startswith('~$')]
                structured_files = [f for f in all_files if f.lower().endswith('.xlsx')]
                unstructured_files = [f for f in all_files if f.lower().endswith(('.pdf', '.docx', '.txt'))]
            except Exception as e:
                logging.error(f"Failed to read directory: {e}")
                return
            
            logging.info(f"Found {len(structured_files)} XLSX files and {len(unstructured_files)} unstructured files.")

            # Process Structured files (XLSX)
            for filename in sorted(structured_files):
                filepath = os.path.join(MOMBASA_DIR, filename)
                fn_lower = filename.lower()

                # --- File Type Routing ---
                
                if 'auctionsummary' in fn_lower:
                    process_auction_summary(filepath, filename, conn)
                
                elif 'generalreport' in fn_lower:
                    process_standard_format(
                        filepath, filename, conn, 
                        data_type=DATA_TYPE_SALE, 
                        target_sheet='General Report', 
                        clean_second_row=True,
                        use_internal_metadata=True
                    )

                elif 'completeofferlots' in fn_lower:
                    process_complete_offer_lots(filepath, filename, conn)
                
                elif 'sale' in fn_lower and 'catalogue' in fn_lower:
                    process_standard_format(filepath, filename, conn, data_type=DATA_TYPE_OFFER)
                
                elif 'auction quantity' in fn_lower:
                     logging.info(f"\n[INFO] Skipping time-series file: {filename}")
                
                else:
                    logging.info(f"\n[INFO] Skipping unrecognized XLSX file format: {filename}")

            # Process Unstructured files (PDF/DOCX/TXT)
            for filename in sorted(unstructured_files):
                 # Skip the diagnostic files if present
                 if filename.lower() in ['header diagnostic.txt', 'mombasa i.txt']:
                     continue
                 filepath = os.path.join(MOMBASA_DIR, filename)
                 process_unstructured_report(filepath, filename, conn)


    except sqlite3.Error as e:
        logging.critical(f"Database connection failed: {e}")
    
    end_time = time.time()
    logging.info(f"\n--- Finished Processor. Total time: {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    # Dependency checks
    try:
        import openpyxl
    except ImportError:
        logging.error("The 'openpyxl' library is required. Please install it: pip install pandas openpyxl")
        exit(1)

    if not fitz or not docx:
        logging.warning("\n[NOTICE] Optional dependencies for PDF (PyMuPDF) or DOCX (python-docx) are missing.")
        logging.warning("To process unstructured reports, please install them: pip install PyMuPDF python-docx\n")
        
    if os.path.exists(DB_FILE):
        logging.warning("\n*** IMPORTANT ***")
        logging.warning("market_reports.db exists. It is strongly recommended to DELETE the existing DB file")
        logging.warning("before running this script to ensure the new UPSERT and COALESCE logic functions correctly on a fresh import.")
        logging.warning("***************\n")
        
    run_processor()