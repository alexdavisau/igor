import os
import requests
import logging
# Import urllib3 to suppress the insecure request warning
import urllib3

# ==============================================================================
# ✅ --- User Configuration ---
# All the variables you need to change are in this section.
# ==============================================================================

# 1. The base URL for your Alation instance.
BASE_URL = "https://northstar.mtse.alationcloud.com"

# 2. The unique path to the specific result you want to download.
# Find this in your browser's address bar when viewing a query result.
# Example: "/schedule/result/2449/"
TARGET_URL_SUFFIX = "/schedule/result/2449/"

# 3. The desired download path and filename.
# You can use '~' to refer to your home directory (e.g., '~/Downloads/results.csv')
OUTPUT_FILE_PATH = "alation_results.csv"

# ==============================================================================
# --- Script Logic (No changes needed below this line) ---
# ==============================================================================

# Suppress the warning that is printed to the console when verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_results_csv_url(result_url_suffix: str, headers: dict) -> str | None:
    """
    Takes a result URL suffix, follows an expected 302 redirect,
    and constructs a direct download link for the corresponding CSV data.
    """
    if not result_url_suffix:
        logging.error("The 'result_url_suffix' cannot be empty.")
        return None

    initial_url = f"{BASE_URL}{result_url_suffix}"
    logging.info("Attempting to get redirect from: %s", initial_url)

    try:
        response = requests.get(
            initial_url,
            headers=headers,
            allow_redirects=False,
            verify=False  # Bypasses the SSL certificate check.
        )

        if response.status_code == 302:
            redirect_location = response.headers.get("Location")
            if not redirect_location:
                logging.error("Redirect (302) received, but no 'Location' header was found.")
                return None
            
            logging.info("Successfully received redirect to: %s", redirect_location)
            
            if "/execution_result/" in redirect_location:
                ajax_url_part = redirect_location.replace("/execution_result/", "/ajax/get_result_table_data/")
                final_url = f"{BASE_URL}{ajax_url_part}?csv=1"
                logging.info("Constructed final CSV download URL: %s", final_url)
                return final_url
            else:
                logging.warning("Redirect location was '%s', which does not contain '/execution_result/'. The URL manipulation logic may need adjustment.", redirect_location)
                return None

        else:
            logging.error("Expected a 302 redirect, but got status code: %d", response.status_code)
            logging.error("Response Body: %s", response.text)
            return None

    except requests.exceptions.RequestException as e:
        logging.exception("An error occurred during the request: %s", e)
        return None

def download_csv(url: str, output_path: str, headers: dict):
    """Downloads a file from a URL and saves it to the specified path."""
    expanded_path = os.path.expanduser(output_path)
    
    logging.info(f"Attempting to download CSV from {url}")
    try:
        with requests.get(url, headers=headers, verify=False, stream=True) as r:
            r.raise_for_status()
            with open(expanded_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        print(f"\n✅ Download complete! File saved to: {expanded_path}")

    except requests.exceptions.RequestException as e:
        print(f"\n❌ A network error occurred during download: {e}")
    except IOError as e:
        print(f"\n❌ A file error occurred. Could not write to '{expanded_path}'. Error: {e}")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred during download: {e}")

def main():
    """Main function to configure and run the script."""
    api_token = os.environ.get('ALATION_API_TOKEN')
    if not api_token:
        logging.error("FATAL: The 'ALATION_API_TOKEN' environment variable is not set.")
        return

    request_headers = {
        'User-Agent': 'Alation-API-Script/1.0',
        'TOKEN': api_token,
        'accept': 'application/json'
    }

    # --- Step 1: Find the CSV download URL using top-level variables ---
    csv_download_url = get_results_csv_url(TARGET_URL_SUFFIX, request_headers)

    if csv_download_url:
        print(f"\n✅ Success! Found the CSV download URL: {csv_download_url}")
        
        # --- Step 2: Download the file using top-level variables ---
        download_csv(csv_download_url, OUTPUT_FILE_PATH, request_headers)
        
    else:
        print("\n❌ Failure. The script did not find the CSV URL.")
        print("   Check the logs above for details on what went wrong.")

if __name__ == "__main__":
    main()
