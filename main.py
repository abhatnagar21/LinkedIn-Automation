from serpapi import GoogleSearch
import csv
import time
import threading
import os
from tqdm import tqdm

# Function to retrieve LinkedIn URL with retry mechanism
def get_linkedin_url_serpapi(company_name, api_key, max_retries=3):
    params = {
        "engine": "google",
        "q": f"{company_name} LinkedIn site:linkedin.com/company",
        "api_key": api_key
    }
    
    for attempt in range(max_retries):
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            for result in results.get("organic_results", []):
                if "linkedin.com/company" in result["link"]:
                    return result["link"], "Success"
            return "LinkedIn URL not found", "Not Found"
        except Exception as e:
            log_error(company_name, str(e))  # Log errors to file
            time.sleep(2)  # Wait before retrying
    return "LinkedIn URL not found", "Failed"

# Error logging function
def log_error(company_name, error_message):
    with open("error_log.txt", "a") as file:
        file.write(f"Error fetching {company_name}: {error_message}\n")

# Load companies from CSV file
def load_companies_from_csv(file_path):
    with open(file_path, mode="r") as file:
        reader = csv.reader(file)
        return [row[0] for row in reader]

# Save results to CSV file
def save_results_to_csv(results, output_file):
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Company Name", "LinkedIn URL", "Status"])
        writer.writerows(results)

# Check if results exist to avoid duplicate API calls
def load_cached_results(output_file):
    cached_results = {}
    if os.path.exists(output_file):
        with open(output_file, mode="r") as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            for row in reader:
                if row:
                    company, url, status = row
                    cached_results[company] = (url, status)
    return cached_results

# Main function with multi-threading
def fetch_linkedin_urls(companies, api_key, output_file="linkedin_urls.csv"):
    cached_results = load_cached_results(output_file)
    results = []
    
    def process_company(company):
        if company in cached_results:
            url, status = cached_results[company]
        else:
            url, status = get_linkedin_url_serpapi(company, api_key)
        results.append((company, url, status))

    # Use threading to speed up requests
    threads = []
    with tqdm(total=len(companies), desc="Fetching LinkedIn URLs") as pbar:
        for company in companies:
            thread = threading.Thread(target=lambda: [process_company(company), pbar.update(1)])
            threads.append(thread)
            thread.start()
            if len(threads) >= 10:  # Limit the number of concurrent threads
                for t in threads:
                    t.join()
                threads = []  # Reset thread list
    
    for t in threads:
        t.join()  # Wait for remaining threads

    save_results_to_csv(results, output_file)
    print(f"\nLinkedIn URLs saved to {output_file}")

# User settings
API_KEY = "b21d8e6a467b3f8fa26a9a8b3d61f49db6de0d8e3066ada886f379ddd3131e00"  # Replace with your SerpAPI key
INPUT_CSV = "Companies - Sheet1.csv"  # CSV file with company names
OUTPUT_CSV = "linkedin_urls.csv"

# Load companies from CSV and fetch LinkedIn URLs
companies = load_companies_from_csv(INPUT_CSV)
fetch_linkedin_urls(companies, API_KEY, OUTPUT_CSV)
