import os
import csv
import re

# --- Configuration ---
CSV_FILE = "adresywfs.csv"
BASE_DIR = "wfs_data"
EXPECTED_FILES = ["ms_budynki.gml", "ms_dzialki.gml"]

def sanitize_filename(name):
    """
    Removes invalid characters from a string to make it a valid filename.
    Should be the same function as in the download script.
    """
    return re.sub(r'[\\/*?:"<>|]',"", name).strip()

def check_downloads():
    """
    Checks for missing directories and files based on the CSV input.
    """
    print(f"Starting verification based on {CSV_FILE}...")
    
    missing_directories = []
    missing_files_report = {}
    
    try:
        with open(CSV_FILE, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile, delimiter=';')
            
            organ_header = next((h for h in reader.fieldnames if 'Organ zgłaszający' in h), None)
            if not organ_header:
                print("ERROR: Could not find the 'Organ zgłaszający' column in the CSV.")
                return

            # Get all expected directories from the CSV
            expected_organs = {row[organ_header].strip() for row in reader if row[organ_header].strip()}

            for organ_name in sorted(expected_organs):
                sanitized_name = sanitize_filename(organ_name)
                organ_dir = os.path.join(BASE_DIR, sanitized_name)

                if not os.path.isdir(organ_dir):
                    missing_directories.append(organ_name)
                else:
                    missing_files = []
                    for file in EXPECTED_FILES:
                        if not os.path.exists(os.path.join(organ_dir, file)):
                            missing_files.append(file)
                    
                    if missing_files:
                        missing_files_report[organ_name] = missing_files

        # --- Generate Report ---
        print("\n--- Verification Report ---")

        if not missing_directories and not missing_files_report:
            print("✅ All directories and files are present. Verification successful!")
        else:
            if missing_directories:
                print("\n❌ Missing Directories:")
                for name in missing_directories:
                    print(f"  - {name}")
            
            if missing_files_report:
                print("\n❌ Missing Files in Existing Directories:")
                for organ, files in missing_files_report.items():
                    print(f"  - In '{organ}': missing {', '.join(files)}")

    except FileNotFoundError:
        print(f"ERROR: The file {CSV_FILE} was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    check_downloads()
