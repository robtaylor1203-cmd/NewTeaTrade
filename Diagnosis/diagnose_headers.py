import pandas as pd
import os
import sys

# Configuration
# IMPORTANT: Ensure this path is correct for your local setup.
MOMBASA_DIR = r"C:\Users\mikin\projects\NewTeaTrade\Mombasa"

def diagnose_headers():
    print(f"--- Starting Excel Header Diagnostics ---")
    print(f"Scanning directory: {MOMBASA_DIR}\n")
    
    if not os.path.exists(MOMBASA_DIR):
        print(f"[ERROR] Directory not found: {MOMBASA_DIR}")
        return

    try:
        files = os.listdir(MOMBASA_DIR)
    except Exception as e:
        print(f"[ERROR] Failed to read directory: {e}")
        return

    # We are interested in all XLSX files now (AuctionSummary and Auction Quantity)
    # Exclude temporary Excel files starting with '~'
    xlsx_files = [f for f in files if f.lower().endswith('.xlsx') and not f.startswith('~')]

    if not xlsx_files:
        print("[INFO] No XLSX files found in the directory.")
        return

    for filename in sorted(xlsx_files):
        filepath = os.path.join(MOMBASA_DIR, filename)
        print(f"\n--------------------------------------------------")
        print(f"FILE: {filename}")
        print(f"--------------------------------------------------")
        
        try:
            # Use ExcelFile to inspect sheet names first
            # engine='openpyxl' is required for .xlsx files
            xls = pd.ExcelFile(filepath, engine='openpyxl')
            
            for sheetname in xls.sheet_names:
                print(f"\n  SHEET: '{sheetname}'")
                try:
                    # Read the first 5 rows to understand the structure and headers
                    # By default, pandas treats the first row (index 0) as the header.
                    df = pd.read_excel(xls, sheet_name=sheetname, nrows=5)
                    
                    if df.empty:
                        print("    -> Status: Sheet is empty.")
                    else:
                         # Convert headers to a list and print
                        headers = [str(col) for col in df.columns.tolist()]
                        print(f"    -> Headers Found (Row 1): {headers}")
                        
                        # Print the first few rows as a table for context
                        print("\n    -> Data Preview (First 5 rows):")
                        # Use to_markdown for clean table printing (requires 'tabulate' package)
                        print(df.to_markdown(index=False))
                        print("\n")

                except Exception as sheet_error:
                    print(f"    [ERROR] Failed to read sheet '{sheetname}': {sheet_error}")

        except Exception as e:
            # Catch errors related to reading the Excel file itself (e.g., corruption, permissions)
            print(f"  [ERROR] Failed to read Excel file: {e}")

    print(f"\n--- Finished Diagnostics ---")
    print("Please copy the output above and send it back for analysis.")

if __name__ == "__main__":
    # Ensure required dependencies are installed
    try:
        import openpyxl
        # Check if tabulate is installed for better table printing
        try:
            import tabulate
        except ImportError:
            print("[INFO] Optional 'tabulate' package not found. Table previews might look slightly different.")
            print("[INFO] You can install it via: pip install tabulate\n")

    except ImportError:
        print("\n[DEPENDENCY ERROR] The 'openpyxl' library is required.")
        print("Please install it: pip install pandas openpyxl\n")
        sys.exit(1)
        
    diagnose_headers()