import requests
import os
import csv
import re
import concurrent.futures

# --- Configuration ---
LAYERS = ["ms:budynki", "ms:dzialki"]
OUTPUT_DIR = "wfs_data"
OUTPUT_FORMAT = "application/gml+xml; version=3.2"
CSV_FILE = "adresywfs.csv"
MAX_WORKERS = 10  # Number of parallel downloads

# --- Main Script ---
def download_wfs_data(url, layer_name, output_dir, output_format):
    """
    Downloads WFS data for a specific layer. This function is designed to be thread-safe.
    """
    # Construct the GetFeature request URL
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": layer_name,
        "outputFormat": output_format,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        # Ensure the output directory exists (thread-safe)
        os.makedirs(output_dir, exist_ok=True)

        sanitized_layer_name = layer_name.replace(":", "_")
        output_filename = os.path.join(output_dir, f"{sanitized_layer_name}.gml")

        with open(output_filename, "wb") as f:
            f.write(response.content)

        print(f"  -> SUCCESS: Saved {layer_name} from {url} to {output_filename}")
        return True

    except requests.exceptions.Timeout:
        print(f"  -> TIMEOUT: for {layer_name} from {url}")
        return False
    except requests.exceptions.RequestException as e:
        if e.response is not None and "LayerNotDefined" in e.response.text:
            print(f"  -> INFO: Layer {layer_name} not found on {url}. Skipping.")
        else:
            print(f"  -> ERROR: Could not download {layer_name} from {url}. Reason: {e}")
        return False
    except Exception as e:
        print(f"  -> UNEXPECTED ERROR: for {layer_name} from {url}. Reason: {e}")
        return False

def sanitize_filename(name):
    """
    Removes invalid characters from a string to make it a valid filename.
    """
    return re.sub(r'[\\/*?:"<>|]',"", name).strip()

def process_wfs_service(organ_name, wfs_url):
    """
    Worker function to process all layers for a single WFS service.
    """
    if not wfs_url or not organ_name:
        print(f"Skipping row due to missing URL or Organ name.")
        return

    print(f"Processing: {organ_name}")
    
    sanitized_organ_name = sanitize_filename(organ_name)
    area_specific_dir = os.path.join(OUTPUT_DIR, sanitized_organ_name)

    for layer in LAYERS:
        download_wfs_data(wfs_url, layer, area_specific_dir, OUTPUT_FORMAT)

def process_csv_parallel(csv_path):
    """
    Processes the CSV file to download WFS data in parallel.
    """
    print(f"Starting parallel download process (max_workers={MAX_WORKERS}) for all entries in {csv_path}...")

    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile, delimiter=';')
            
            url_header = next((h for h in reader.fieldnames if 'Usługa pobierania' in h), None)
            organ_header = next((h for h in reader.fieldnames if 'Organ zgłaszający' in h), None)

            if not url_header or not organ_header:
                print("ERROR: Could not find the required columns ('Organ zgłaszający', 'Usługa pobierania') in the CSV.")
                return

            # Use ThreadPoolExecutor to run downloads in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Create a list of tasks to submit
                futures = []
                for row in reader:
                    wfs_url = row[url_header].strip()
                    organ_name = row[organ_header].strip()
                    if wfs_url and organ_name:
                        # Submit the worker function to the executor
                        futures.append(executor.submit(process_wfs_service, organ_name, wfs_url))
                
                # Wait for all futures to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()  # Retrieve result to raise any exceptions that occurred in the thread
                    except Exception as exc:
                        print(f'A task generated an exception: {exc}')

    except FileNotFoundError:
        print(f"ERROR: The file {csv_path} was not found.")
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")


if __name__ == "__main__":
    process_csv_parallel(CSV_FILE)
    print("\nAll download tasks have been processed.")
