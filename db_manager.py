# db_manager.py
import sqlite3
import json
import logging

DATABASE_NAME = 'market_data.db'
JSON_OUTPUT_FILE = 'market-reports-library.json'

def initialize_database():
    logging.info(f"Initializing database: {DATABASE_NAME}")
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT, auction_centre TEXT NOT NULL,
            week_number INTEGER, year INTEGER NOT NULL, source TEXT NOT NULL, report_link TEXT NOT NULL UNIQUE,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, report_id INTEGER, lot_number TEXT, garden_name TEXT, tea_grade TEXT,
            quantity_kg REAL, price REAL, FOREIGN KEY(report_id) REFERENCES reports(id)
        )''')
        conn.commit()
    logging.info("Database initialized successfully.")

def insert_report_data(report_metadata, granular_data_df):
    report_id = None
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO reports (title, description, auction_centre, week_number, year, source, report_link)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                report_metadata['title'], report_metadata['description'], report_metadata['auction_centre'],
                report_metadata.get('week_number'), report_metadata['year'], report_metadata['source'],
                report_metadata['report_link']
            ))
            report_id = cursor.lastrowid
            logging.info(f"DB Manager: Inserted metadata for '{report_metadata['title']}' (ID: {report_id})")

            if report_id and not granular_data_df.empty:
                granular_data_df['report_id'] = report_id
                db_columns = ['report_id', 'lot_number', 'garden_name', 'tea_grade', 'quantity_kg', 'price']
                for col in db_columns:
                    if col not in granular_data_df.columns:
                        granular_data_df[col] = None
                df_to_insert = granular_data_df[db_columns]
                df_to_insert.to_sql('results', conn, if_exists='append', index=False)
                logging.info(f"DB Manager: Inserted {len(df_to_insert)} granular rows for report_id: {report_id}")
            
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            logging.warning(f"DB Manager: Report link already exists, skipping: {report_metadata['report_link']}")
            return False
        except Exception as e:
            logging.error(f"DB Manager: Database error for report '{report_metadata['title']}'. Rolling back. Error: {e}")
            conn.rollback()
            return False

def build_reports_json():
    logging.info("DB Manager: Building JSON file for the website.")
    reports_list = []
    try:
        with sqlite3.connect(DATABASE_NAME) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            query = "SELECT title, description, auction_centre, week_number, year, source, report_link FROM reports ORDER BY year DESC, week_number DESC"
            cursor.execute(query)
            rows = cursor.fetchall()
            reports_list = [dict(row) for row in rows]
    except Exception as e:
        logging.error(f"DB Manager: Failed to query database to build JSON. Error: {e}")
        return

    with open(JSON_OUTPUT_FILE, 'w') as f:
        json.dump(reports_list, f, indent=4)
    
    if reports_list:
        logging.info(f"DB Manager: Successfully created {JSON_OUTPUT_FILE} with {len(reports_list)} reports.")
    else:
        logging.warning(f"DB Manager: Created empty {JSON_OUTPUT_FILE}.")