import time
import pandas as pd
from bs4 import BeautifulSoup
import random
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext, Checkbutton, IntVar
import queue
import os
import glob
import csv  # Import CSV for logging
import datetime # Import datetime for timestamps
import re # Import regex for cleaning filenames

# Import selenium components
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# The base URL of the website
BASE_URL = "https://www.legal500.com"

# --- UPDATED: 18 modern user-agents ---
USER_AGENTS = [
    # Chrome (Win, Mac, Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox (Win, Mac, Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    # Edge (Win, Mac)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    # Safari (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    # Older but common
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.41",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
]

# --- Global Flags & Shared Resources ---
exit_requested = False
log_queue = queue.Queue()
scraper_thread_running = False # Flag to track scraper status

# --- UPDATED: CSV Log Opener (now takes folder path) ---
def open_csv_writer(region_folder_path, region_name, mode='w'):
    """
    Opens a CSV file for a given region *inside its folder* and returns the file and writer objects.
    'w' mode is for writing (creates new file/overwrites).
    'a' mode is for appending (adds to existing file).
    """
    safe_region_name = re.sub(r'[\\/*?:"<>|]', "", region_name)
    csv_filename = f"log_region_{safe_region_name}.csv"
    # UPDATED: Use os.path.join to create path inside the folder
    full_csv_path = os.path.join(region_folder_path, csv_filename)
    
    # Use a symbolic log message for the GUI
    log_queue.put( (f"📝 Opening regional log: {full_csv_path} (Mode: {mode})", "info") )
    
    # Ensure encoding is 'utf-8' for broad compatibility
    csv_file = open(full_csv_path, mode, newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    
    # Only write the header if we are in 'write' mode (new file)
    if mode == 'w':
        # UPDATED Header: Removed Logo URL, added detailed columns
        csv_writer.writerow(["FirmName", "Timestamp", "LogLevel", "Practice Area", "Ranking Table", "Firm", "Sourcelink"])
        
    return csv_file, csv_writer

# --- NEW: Simple CSV Log Writer ---
def write_simple_csv_log(csv_writer, firm_name, level, message):
    """
    Helper function to write a simple status log entry (like 'Starting scrape').
    It fills the main message columns and leaves the detail columns blank.
    """
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Sanitize message just in case it contains commas or newlines
        clean_message = str(message).replace('"', '""').replace('\n', ' ')
        
        # Get the new 7-column row format
        # We put the simple message in the "LogLevel" column as it's a general status
        # FirmName, Timestamp, LogLevel(Message), PA, Tier, Firm, Link
        csv_writer.writerow([firm_name, timestamp, level, f'"{clean_message}"', "", "", ""])
    except Exception as e:
        # If logging fails, print to GUI queue to avoid crashing
        log_queue.put( (f"!! CSV LOGGING FAILED: {e} !!", "error") )


# --- GUI Application Class ---
class ScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Legal 500 Scraper")
        self.root.protocol("WM_DELETE_WINDOW", self.on_exit)

        self.regions_data = {}
        self.check_vars = {}

        # --- Frames ---
        control_frame = tk.Frame(root, padx=10, pady=10)
        control_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        log_frame = tk.Frame(root, padx=10, pady=10)
        log_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # --- Controls ---
        tk.Label(control_frame, text="Select Regions to Scrape:", font=("Helvetica", 12, "bold")).pack(anchor='w')
        
        self.region_frame = tk.Frame(control_frame, borderwidth=1, relief="sunken")
        self.region_frame.pack(fill='both', expand=True, pady=5)
        
        tk.Button(control_frame, text="Select All", command=self.select_all).pack(fill='x', pady=2)
        tk.Button(control_frame, text="Deselect All", command=self.deselect_all).pack(fill='x', pady=2)
        
        self.start_button = tk.Button(control_frame, text="Start Scraping", command=self.start_scraping, bg="green", fg="white", font=("Helvetica", 10, "bold"))
        self.start_button.pack(fill='x', pady=10)
        
        self.exit_button = tk.Button(control_frame, text="Exit", command=self.on_exit, bg="red", fg="white")
        self.exit_button.pack(fill='x', side=tk.BOTTOM, pady=5)

        # --- Log Area ---
        tk.Label(log_frame, text="Log:", font=("Helvetica", 12, "bold")).pack(anchor='w')
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state='disabled', font=("Courier New", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # --- NEW: Define color tags ---
        self.log_text.tag_configure("header", foreground="#6a0dad", font=("Courier New", 9, "bold"))
        self.log_text.tag_configure("info", foreground="#007acc")
        self.log_text.tag_configure("success", foreground="#00994d")
        self.log_text.tag_configure("warning", foreground="#ff8c00")
        self.log_text.tag_configure("error", foreground="#d40000", font=("Courier New", 9, "bold"))
        
        self.root.after(100, self.process_log_queue)
        
        # --- Fetch regions on startup ---
        threading.Thread(target=self.populate_regions, daemon=True).start()

    def log(self, message, tag="info"):
        # --- TCLERROR FIX: Wrap in try/except ---
        # This catches errors if the log runs *after* the window is destroyed
        try:
            self.log_text.configure(state='normal')
            self.log_text.insert(tk.END, message + '\n', (tag,))
            self.log_text.configure(state='disabled')
            self.log_text.see(tk.END)
        except tk.TclError:
            pass # Ignore errors trying to write to a destroyed widget

    def process_log_queue(self):
        while not log_queue.empty():
            try:
                message_tuple = log_queue.get_nowait()
                if isinstance(message_tuple, tuple):
                    message, tag = message_tuple
                else:
                    message, tag = message_tuple, "info" # Default tag
                
                # --- NEW: Assign tag based on content ---
                if message.startswith("✅"): tag = "success"
                elif message.startswith("❌"): tag = "error"
                elif message.startswith("⚠️"): tag = "warning"
                elif message.startswith("💾") or message.startswith("🕵️") or message.startswith("📝"): tag = "info"
                elif message.startswith("🏁") or message.startswith("🛑") or message.startswith("=") or message.startswith("─"): tag = "header"
                elif message.startswith("⚙️") or message.startswith("📍"): tag = "header"
                
                self.log(message, tag)
                
            except tk.TclError:
                pass # Ignore errors if window is destroyed while queue is full
        
        # New logic: Check if thread finished, then allow exit
        global scraper_thread_running
        if not scraper_thread_running and exit_requested:
             try:
                self.root.destroy() # Now it's safe to destroy
             except tk.TclError:
                pass # Window already gone
             return # --- TCLERROR FIX: Stop scheduling new checks ---
        
        self.root.after(100, self.process_log_queue)

    def populate_regions(self):
        log_queue.put( ("🗺️  Fetching UK regions...", "info") )
        self.regions_data = get_uk_regions()
        if self.regions_data:
            for name in sorted(self.regions_data.keys()):
                var = tk.IntVar()
                cb = tk.Checkbutton(self.region_frame, text=name, variable=var)
                cb.pack(anchor='w')
                self.check_vars[name] = var
            log_queue.put( (f"✅  Found {len(self.regions_data)} regions. Please make a selection.", "success") )
        else:
            log_queue.put( ("❌  Failed to fetch regions. Please check connection and restart.", "error") )

    def select_all(self):
        for var in self.check_vars.values():
            var.set(1)

    def deselect_all(self):
        for var in self.check_vars.values():
            var.set(0)

    def start_scraping(self):
        global scraper_thread_running
        selected_regions = [name for name, var in self.check_vars.items() if var.get() == 1]
        if not selected_regions:
            messagebox.showwarning("No Selection", "Please select at least one region to scrape.")
            return

        if scraper_thread_running:
            messagebox.showwarning("In Progress", "Scraper is already running.")
            return

        self.start_button.config(state='disabled', text="Scraping...")
        self.exit_button.config(text="Request Exit")
        
        scraper_thread_running = True # Set flag
        
        # Pass root object to the thread so it can signal completion
        # Thread is NOT a daemon, so app will wait for it.
        threading.Thread(target=run_scraper, args=(selected_regions, self.regions_data, self.start_button, self.root)).start()

    def on_exit(self):
        global exit_requested
        
        if not scraper_thread_running:
            # If not running, just destroy the window
            if messagebox.askokcancel("Exit", "Are you sure you want to exit?"):
                try:
                    self.root.destroy()
                except tk.TclError:
                    pass # Window already gone
        else:
            # If running, set the flag and let the scraper handle the shutdown
            if messagebox.askokcancel("Exit", "Are you sure you want to exit? The current process will be stopped safely to save all data."):
                exit_requested = True
                log_queue.put( ("\n-- Exit requested by user! The script will shut down safely. --", "warning") )
                self.exit_button.config(state='disabled', text="Exiting...")
                self.start_button.config(state='disabled', text="Exiting...")


# --- Scraper Functions ---

def get_uk_regions():
    """Fetches all available UK regions from the main rankings page."""
    temp_driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--headless") # Consider running headless
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        service = Service(ChromeDriverManager().install())
        temp_driver = webdriver.Chrome(service=service, options=chrome_options)
        temp_driver.get(f"{BASE_URL}/rankings#r/united-kingdom")

        log_queue.put( ("...Connecting to Legal500, please wait...", "info") )
        time.sleep(5)
        wait = WebDriverWait(temp_driver, 20)
        try:
            accept_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Accept All']")))
            accept_button.click()
            log_queue.put( ("...Accepted cookie banner.", "info") )
            time.sleep(2)
        except TimeoutException: pass
        log_queue.put( ("...Locating region list...", "info") )
        region_list_ul = wait.until(EC.visibility_of_element_located((By.XPATH, "//h4[text()='Solicitors']/following-sibling::ul")))
        soup = BeautifulSoup(region_list_ul.get_attribute('outerHTML'), 'html.parser')
        return {link.text.strip(): link.get('href') for link in soup.find_all('a')}
    except Exception as e:
        log_queue.put( (f"❌ ERROR: Failed to fetch UK regions: {e}", "error") )
        return None
    finally:
        if temp_driver: temp_driver.quit()

# --- UPDATED: Now takes folder path ---
def consolidate_backup_files(region_folder_path, region_name):
    """Finds, merges, and deletes backup Excel files for a given region *inside its folder*."""
    safe_region_name = region_name.replace(' ', '_')
    # UPDATED: Use os.path.join
    main_filename = os.path.join(region_folder_path, f"{safe_region_name}_rankings.xlsx")
    backup_pattern = os.path.join(region_folder_path, f"{safe_region_name}_rankings_*.xlsx")
    
    backup_files = glob.glob(backup_pattern)
    if not backup_files: return
    log_queue.put( (f"\n 🗂️  Found {len(backup_files)} backup file(s) for {region_name}. Consolidating...", "info") )
    all_dfs = []
    if os.path.exists(main_filename):
        try:
            all_dfs.append(pd.read_excel(main_filename))
        except Exception as e:
            log_queue.put( (f"   ⚠️ Could not read main file '{main_filename}': {e}", "warning") )
            
    for backup in backup_files:
        try:
            all_dfs.append(pd.read_excel(backup))
        except Exception as e:
            log_queue.put( (f"   ⚠️ Could not read backup file '{backup}': {e}", "warning") )

    if not all_dfs:
        log_queue.put( ("   - No data found in backup files.", "info") )
        return
        
    consolidated_df = pd.concat(all_dfs, ignore_index=True)
    consolidated_df.drop_duplicates(subset=['Sourcelink'], keep='last', inplace=True)
    try:
        consolidated_df.to_excel(main_filename, index=False)
        log_queue.put( (f"   ✅  Successfully merged all data into '{main_filename}'.", "success") )
        for backup in backup_files:
            try:
                os.remove(backup)
            except Exception as e:
                log_queue.put( (f"   ⚠️ Could not remove backup file '{backup}': {e}", "warning") )
        log_queue.put( ("   🗑️  Cleaned up old backup files.", "info") )
    except Exception as e:
        log_queue.put( (f"   ❌ ERROR: Could not save consolidated file. Error: {e}", "error") )

# --- UPDATED: Now takes folder path ---
def save_regional_data(region_folder_path, data, region_name, firm_name, current_total):
    """Saves data for a specific region *inside its folder*, handling permission errors."""
    if not data: return
    
    safe_region_name = region_name.replace(' ', '_')
    # UPDATED: Use os.path.join
    filename = os.path.join(region_folder_path, f"{safe_region_name}_rankings.xlsx")
    
    # Updated GUI log message format
    log_queue.put( (f"\n💾 Saving {current_total} rankings for {firm_name} to {filename}...", "info") ) 

    try:
        df = pd.DataFrame(data)
        # UPDATED: Removed 'Logo URL'
        columns = ["Region", "Ranking Location", "Practice Area", "Ranking Table", "Firm", "Sourcelink"]
        for col in columns:
            if col not in df.columns: df[col] = "N/A"
        df = df[columns]
        df.to_excel(filename, index=False)
        log_queue.put( (f"✅ Data for {firm_name} saved successfully.", "success") ) # Updated GUI log
        
    except PermissionError:
        log_queue.put( (f"   ❌ ERROR: Permission denied for '{filename}'.", "error") )
        log_queue.put( ("     Please make sure the file is not open in Excel.", "error") )
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        # UPDATED: Use os.path.join
        fallback_filename = os.path.join(region_folder_path, f"{safe_region_name}_rankings_{timestamp}.xlsx")
        log_queue.put( (f"   ↪️ Saving to a new file: '{fallback_filename}'", "warning") )
        try:
            df.to_excel(fallback_filename, index=False)
        except Exception as e:
            log_queue.put( (f"   ❌ FATAL: Could not save to fallback file. Error: {e}", "error") )
    except Exception as e:
        log_queue.put( (f"   ❌ ERROR: An unexpected error occurred while saving. Error: {e}", "error") )

def initialize_driver():
    """Creates and returns a new Selenium WebDriver instance."""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    # Add options to clear cache
    chrome_options.add_argument('--disk-cache-dir=/tmp/cache')
    chrome_options.add_argument('--media-cache-dir=/tmp/media-cache')
    
    # UPDATED: Pick from the larger list
    selected_user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"user-agent={selected_user_agent}")
    log_queue.put( (f"🕵️  Initializing new browser session (User-Agent: ...{selected_user_agent[-30:]})", "info") )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def handle_cookies_if_present(driver):
    """Checks for and clicks the 'Accept All' cookie banner if it appears."""
    try:
        cookie_wait = WebDriverWait(driver, 3)
        accept_button = cookie_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Accept All']")))
        log_queue.put( ("   🍪 Cookie banner appeared. Clicking 'Accept All'.", "info") )
        accept_button.click()
        time.sleep(random.uniform(1, 2))
    except TimeoutException: pass

def extract_ranking_data(driver, wait, ranking_location, region_name, data_list, csv_writer, firm_name):
    """
    Extracts data from a single ranking page.
    Appends data to data_list (for excel).
    Writes a detailed log row to csv_writer (for logging).
    """
    
    # This dict is for the main excel data
    extracted_data = {}
    
    # This list is for the detailed CSV log row
    log_statuses = {
        "Practice Area": "Pending",
        "Ranking Table": "Pending",
        "Firm": "Pending",
        "Sourcelink": "Pending"
    }
    
    current_url = "N/A" # Default in case of immediate crash
    
    try:
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "header")))
        page_soup = BeautifulSoup(driver.page_source, 'html.parser')
        header = page_soup.find('header', class_='flex flex-col gap-4')
        current_url = driver.current_url
        log_statuses["Sourcelink"] = "Done"

        if not header:
            raise ValueError("Header container not found on ranking page.")

        # --- Extract Practice Area ---
        try:
            pa_element = header.find('h3', class_='typography-heading-s').find('a')
            if pa_element:
                extracted_data["Practice Area"] = pa_element.text.strip()
                log_statuses["Practice Area"] = "Done"
            else:
                extracted_data["Practice Area"] = "N/A"
                log_statuses["Practice Area"] = "Failed: h3/a element not found"
        except Exception as e:
            extracted_data["Practice Area"] = "Failed"
            log_statuses["Practice Area"] = f"Failed: {str(e).splitlines()[0]}" # Short error
        
        # --- Extract Firm Name ---
        try:
            firm_element = header.find('h1', class_='typography-heading-l').find('a')
            if firm_element:
                extracted_data["Firm"] = firm_element.text.strip()
                log_statuses["Firm"] = "Done"
            else:
                extracted_data["Firm"] = "N/A"
                log_statuses["Firm"] = "Failed: h1/a element not found"
        except Exception as e:
            extracted_data["Firm"] = "Failed"
            log_statuses["Firm"] = f"Failed: {str(e).splitlines()[0]}" # Short error

        # --- Extract Ranking Table / Tier (with NEW fallback) ---
        try:
            tier_element = header.find('span', class_='md:typography-interface-l-bold')
            if tier_element:
                extracted_data["Ranking Table"] = tier_element.text.strip()
                log_statuses["Ranking Table"] = "Done"
            else:
                # NEW: Fallback logic for "Firms to watch"
                firms_to_watch_img = header.find('img', alt="Firms to watch")
                if firms_to_watch_img:
                    extracted_data["Ranking Table"] = "Firms to watch"
                    log_statuses["Ranking Table"] = "Done (Firms to watch)"
                else:
                    extracted_data["Ranking Table"] = "N/A"
                    log_statuses["Ranking Table"] = "Failed: Tier span and 'Firms to watch' img not found"
        except Exception as e:
            extracted_data["Ranking Table"] = "Failed"
            log_statuses["Ranking Table"] = f"Failed: {str(e).splitlines()[0]}" # Short error

        # --- Add constant and source link ---
        extracted_data["Region"] = "United Kingdom"
        extracted_data["Ranking Location"] = ranking_location
        extracted_data["Sourcelink"] = current_url
        
        data_list.append(extracted_data)
        
        # Write the detailed log row
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        csv_writer.writerow([
            firm_name, 
            timestamp, 
            "INFO",
            log_statuses["Practice Area"],
            log_statuses["Ranking Table"],
            log_statuses["Firm"],
            log_statuses["Sourcelink"]
        ])

    except Exception as e:
        # This is a critical failure for the *whole page*
        log_queue.put( (f"       - ❌ Error extracting data from tab: {str(e).splitlines()[0]}", "error") ) # Keep minimal error in GUI
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Log the short "Message:" part of the error
        error_msg = str(e).splitlines()[0]
        csv_writer.writerow([
            firm_name, 
            timestamp, 
            "ERROR",
            f"Page Failed: {error_msg}",
            f"Page Failed: {error_msg}",
            f"Page Failed: {error_msg}",
            f"Page Failed: {current_url}"
        ])

def run_scraper(selected_regions, regions_data, start_button, root):
    """Main function to orchestrate the browser navigation and data scraping process."""
    global scraper_thread_running
    driver = None
    all_scraped_data = {}
    
    # --- NEW: Define a root directory for all regions ---
    base_output_dir = "Legal500_Scraped_Data"
    os.makedirs(base_output_dir, exist_ok=True)


    try:
        # --- REMOVED driver = initialize_driver() ---
        # Driver is now initialized *inside* the region loop

        for region_name in selected_regions:
            if exit_requested:
                log_queue.put( (f"\n🛑 Halting before starting {region_name}.", "warning") )
                break
            
            # --- NEW: Create the dedicated folder for this region ---
            safe_region_name_folder = re.sub(r'[\\/*?:"<>|]', "", region_name).replace(' ', '_')
            region_folder_path = os.path.join(base_output_dir, safe_region_name_folder)
            os.makedirs(region_folder_path, exist_ok=True)
            log_queue.put( (f"\n📁 Using directory: {region_folder_path}", "info") )
            
            csv_file = None
            csv_writer = None
            driver = None # Ensure driver is reset
            
            try:
                # --- UPDATED: Pass folder path to CSV writer ---
                csv_file, csv_writer = open_csv_writer(region_folder_path, region_name, mode='w')
                
                # --- UPDATED: Pass folder path to consolidator ---
                consolidate_backup_files(region_folder_path, region_name)
                
                all_scraped_data[region_name] = []
                start_index = 0
                
                # --- UPDATED: Use folder path for filename ---
                filename = os.path.join(region_folder_path, f"{region_name.replace(' ', '_')}_rankings.xlsx")
                
                # --- NEW: Initialize driver *per region* ---
                driver = initialize_driver()
                wait = WebDriverWait(driver, 20)
                
                if os.path.exists(filename):
                    log_queue.put( (f"\n📄 Found existing file for {region_name}. Attempting to resume.", "info") )
                    try:
                        df_existing = pd.read_excel(filename)
                        all_scraped_data[region_name] = df_existing.to_dict('records')
                        scraped_firms = df_existing['Firm'].unique()
                        
                        driver.get(f"{BASE_URL}/{regions_data[region_name]}/directory")
                        all_firm_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'grid')]//article/a//h4")))
                        all_firm_names = [f.text.strip() for f in all_firm_elements]
                        
                        if scraped_firms.size > 0:
                            last_scraped_firm = scraped_firms[-1]
                            try:
                                start_index = all_firm_names.index(last_scraped_firm) + 1
                                log_queue.put( (f"   ↪️ Resuming after '{last_scraped_firm}'. Starting with firm #{start_index + 1}.", "info") )
                            except ValueError:
                                log_queue.put( (f"   ⚠️ Could not find last firm. Starting from the beginning.", "warning") )
                    except Exception as e:
                        log_queue.put( (f"   ❌ ERROR reading existing Excel file. Starting from scratch. Error: {e}", "error") )
                        all_scraped_data[region_name] = []
                        start_index = 0


                log_queue.put( ("\n" + "─"*25 + f"\n 📍 Starting Region: {region_name}\n" + "─"*25, "header") )
                driver.get(f"{BASE_URL}/{regions_data[region_name]}/directory")
                time.sleep(random.uniform(2, 4))
                handle_cookies_if_present(driver)
                ranking_location = wait.until(EC.visibility_of_element_located((By.TAG_NAME, "h1"))).text.strip()
                
                # We get num_firms once at the start to set the loop range
                num_firms = len(wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'grid')]//article/a//h4"))))
                log_queue.put( (f"   ✅ Found {num_firms} firms in {ranking_location}.", "success") )

                session_firm_count = 0
                for i in range(start_index, num_firms):
                    if exit_requested:
                        log_queue.put( ("\n🛑 Halting firm processing loop.", "warning") )
                        break

                    # --- UPDATED: Restart *browser* (not just driver) within region loop ---
                    if session_firm_count > 0 and session_firm_count % 15 == 0:
                        log_queue.put( ("\n" + "─"*15 + " 🔄 SCHEDULED RESTART " + "─"*15, "header") )
                        log_queue.put( ("   Reached 15 firms. Saving log and restarting browser...", "info") )
                        
                        # --- NEW: Save log before restart ---
                        if csv_file and csv_writer:
                            # --- ADDED: Log summary before closing ---
                            total_rankings = len(all_scraped_data[region_name])
                            log_queue.put( (f"   📊 Logging interim total: {total_rankings} rankings for {region_name}.", "info") )
                            write_simple_csv_log(csv_writer, "SCRIPT_STATUS", "INFO", f"Interim save. Total rankings so far: {total_rankings}")
                            # --- END ADDED ---
                            csv_file.close()
                        
                        driver.quit()
                        log_queue.put( ("   Browser closed. Pausing for 2 minutes...", "info") )
                        
                        # Check for exit request during pause
                        for _ in range(120):
                            if exit_requested: break
                            time.sleep(1)
                        if exit_requested: break # Break from inner loop
                        
                        driver = initialize_driver()
                        wait = WebDriverWait(driver, 20)
                        
                        # --- UPDATED: Re-open log in append mode, passing folder path ---
                        csv_file, csv_writer = open_csv_writer(region_folder_path, region_name, mode='a')
                        
                        log_queue.put( (f"   Restarting process for {region_name}...", "info") )
                        driver.get(f"{BASE_URL}/{regions_data[region_name]}/directory")
                        time.sleep(random.uniform(2, 4))
                        handle_cookies_if_present(driver)
                    
                    if exit_requested: break # Break from inner loop

                    
                    firm_name_to_process = "N/A"
                    try:
                        # --- STALE ELEMENT FIX: Re-find elements *inside* the loop ---
                        wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'grid')]")))
                        firms = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'grid')]//article/a//h4")))
                        
                        if i >= len(firms):
                            log_queue.put( (f"   ⚠️ Firm list changed unexpectedly. Skipping to next region.", "warning") )
                            write_simple_csv_log(csv_writer, "N/A", "WARN", "Firm list changed mid-scrape. Breaking region.")
                            break # Break from this region's for-loop

                        firm_name_to_process = firms[i].text.strip()
                        firm_link_element = firms[i].find_element(By.XPATH, "./ancestor::a")

                    except StaleElementReferenceException as e:
                        log_queue.put( (f"   ❌ StaleElement while *finding* firm {i+1}. Skipping firm.", "error") )
                        write_simple_csv_log(csv_writer, "N/A", "ERROR", f"StaleElement finding firm {i+1}. Skipping. Error: {e}")
                        continue # Skip to the next 'i'
                    except TimeoutException as e:
                        log_queue.put( (f"   ❌ Timeout while *finding* firm {i+1}. Skipping firm.", "error") )
                        write_simple_csv_log(csv_writer, "N/A", "ERROR", f"Timeout finding firm {i+1}. Skipping. Error: {e}")
                        continue # Skip to the next 'i'
                    
                    # --- Log to CSV ---
                    write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"--- Starting scrape for {firm_name_to_process} in {region_name} ---")

                    log_queue.put( ("\n" + "="*50 + f"\n⚙️ PROCESSING FIRM {i+1}/{num_firms}: {firm_name_to_process} " + "\n" + "="*50, "header") )
                    
                    try:
                        # --- STALE ELEMENT FAILSAFE: Wrap click in try/except ---
                        ActionChains(driver).move_to_element(firm_link_element).pause(0.5).click().perform()
                    except StaleElementReferenceException as e:
                        log_queue.put( (f"   ❌ StaleElement while *clicking* {firm_name_to_process}. Skipping firm.", "error") )
                        write_simple_csv_log(csv_writer, firm_name_to_process, "ERROR", f"StaleElement on click. Skipping. Error: {e}")
                        continue # Skip to the next 'i'
                    except Exception as e:
                         log_queue.put( (f"   ❌ CRITICAL Error while *clicking* {firm_name_to_process}. Skipping firm. Error: {e}", "error") )
                         error_msg = str(e).splitlines()[0]
                         write_simple_csv_log(csv_writer, firm_name_to_process, "CRITICAL", f"CRITICAL Error on click. Skipping. Error: {error_msg}")
                         continue # Skip to the next 'i'
                    
                    try:
                        time.sleep(random.uniform(2.5, 4.5))
                        # --- UPDATED: More specific wait ---
                        wait.until(EC.visibility_of_element_located((By.XPATH, f"//h1[contains(text(), \"{firm_name_to_process}\")] | //h1[contains(text(), 'The Legal 500')]")))
                        handle_cookies_if_present(driver)
                        
                        # --- UPDATED: Check if we are on the wrong page ---
                        try:
                            # Try to find the firm header
                            driver.find_element(By.XPATH, f"//h1[contains(text(), \"{firm_name_to_process}\")]")
                        except:
                            # If it fails, we are on a practice area page or similar
                            log_queue.put( (f"   ⚠️ Clicked '{firm_name_to_process}' but landed on a generic page. Skipping.", "warning") )
                            write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", "Not a firm page (or timed out). Skipping.")
                            raise Exception("Not a firm page") # Jump to the outer catch block

                        
                        ranking_cards = []
                        try:
                            ranking_cards = driver.find_elements(By.XPATH, "//a[contains(@href, '/rankings/ranking/')]")
                            if not ranking_cards: raise TimeoutException # Trigger fallback if empty
                        except TimeoutException:
                            write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", "Standard ranking card XPath failed. Trying alternative.")
                            try:
                                rankings_container = wait.until(EC.presence_of_element_located((By.XPATH, "//section[contains(@class, 'p-0')]")))
                                ranking_cards = rankings_container.find_elements(By.XPATH, ".//a[contains(@href, '/rankings/ranking/')]")
                            except TimeoutException:
                                 write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", "Alternative ranking card XPath also failed.")
                                 ranking_cards = []
                        
                        num_rankings = len(ranking_cards)
                        
                        if not ranking_cards:
                            log_queue.put( (" 🔍 Found 0 rankings.", "info") )
                            write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", "No ranking entries found.")
                        else:
                            # --- UPDATED: New batching logic ---
                            if num_rankings > 100:
                                batch_size = 10
                            elif num_rankings > 4:
                                batch_size = 4
                            else:
                                batch_size = num_rankings
                            
                            log_queue.put( (f" 🔍 Found {num_rankings} rankings. Setting batch size to {batch_size}.", "info") )
                            write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"Found {num_rankings} rankings. Batch size: {batch_size}.")

                            # --- UPDATED: Check for individual vs batch processing ---
                            if batch_size == num_rankings:
                                # This handles the 1-4 ranking case
                                log_queue.put( (f"\tProcessing {num_rankings} individual rankings...", "info") )
                                original_window = driver.current_window_handle
                                write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"Processing {num_rankings} individual rankings.")
                                cards_opened = 0
                                for j, card in enumerate(ranking_cards):
                                    if exit_requested: break
                                    # --- REMOVED: log_queue.put(f"\t   - Clicking ranking {j+1}/{num_rankings}...") ---
                                    try:
                                        ActionChains(driver).key_down(Keys.CONTROL).click(card).key_up(Keys.CONTROL).perform()
                                        cards_opened += 1
                                        time.sleep(random.uniform(0.8, 1.5))
                                    except Exception as card_e:
                                        error_msg = str(card_e).splitlines()[0]
                                        log_queue.put( (f"   ❌ Error on ranking {j+1}. Skipping. Error: {error_msg}", "error") )
                                        write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", f"Error on ranking card {j+1}. Skipping. Error: {error_msg}")
                                if exit_requested: break

                                try:
                                    wait.until(EC.number_of_windows_to_be(cards_opened + 1))
                                except TimeoutException:
                                    log_queue.put( (f"   ⚠️ Not all {cards_opened} tabs opened. Processing opened tabs.", "warning") )
                                    write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", f"Not all {cards_opened} ranking tabs opened.")

                                open_windows = [w for w in driver.window_handles if w != original_window]
                                for window in open_windows:
                                    if exit_requested: break
                                    driver.switch_to.window(window)
                                    # --- Pass csv_writer and firm_name ---
                                    extract_ranking_data(driver, wait, ranking_location, region_name, all_scraped_data[region_name], csv_writer, firm_name_to_process)
                                    driver.close()
                                if exit_requested: break
                                
                                driver.switch_to.window(original_window)
                                if not exit_requested:
                                    log_queue.put( ("\t✅ All individual rankings processed.", "success") )
                                    write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", "Finished processing individual rankings.")
                            else:
                                # This handles the 5-100 (batch 4) and 101+ (batch 10) cases
                                num_batches = (num_rankings + batch_size - 1) // batch_size
                                log_queue.put( (f"\tProcessing {num_rankings} rankings in {num_batches} batches of {batch_size}...", "info") )
                                write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"Processing in {num_batches} batches.")
                                
                                for batch_num, batch_start in enumerate(range(0, num_rankings, batch_size)):
                                    if exit_requested: break
                                    log_queue.put( (f"\t- Processing Batch {batch_num + 1}/{num_batches}...", "info") ) # <-- GFX LOG
                                    write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"Starting Batch {batch_num + 1}/{num_batches}")
                                    original_window = driver.current_window_handle
                                    
                                    # We must re-find the cards every batch
                                    current_cards = driver.find_elements(By.XPATH, "//a[contains(@href, '/rankings/ranking/')]")[batch_start:min(batch_start + batch_size, num_rankings)]
                                    
                                    cards_opened = 0
                                    for k, card in enumerate(current_cards): # <-- Add counter k
                                        if exit_requested: break
                                        # --- REMOVED: log_queue.put(f"\t   - Clicking ranking {batch_start + k + 1}/{num_rankings}...") ---
                                        try:
                                            ActionChains(driver).key_down(Keys.CONTROL).click(card).key_up(Keys.CONTROL).perform()
                                            cards_opened += 1
                                            time.sleep(random.uniform(0.8, 1.5))
                                        except Exception as card_e:
                                            error_msg = str(card_e).splitlines()[0]
                                            log_queue.put( (f"   ❌ Error on ranking in batch. Skipping. Error: {error_msg}", "error") )
                                            write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", f"Error on ranking card in batch. Skipping. Error: {error_msg}")
                                    if exit_requested: break

                                    try:
                                        wait.until(EC.number_of_windows_to_be(cards_opened + 1))
                                    except TimeoutException:
                                        log_queue.put( (f"   ⚠️ Not all {cards_opened} tabs opened. Processing opened tabs.", "warning") )
                                        write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", f"Not all {cards_opened} ranking tabs opened in batch.")

                                    open_windows = [w for w in driver.window_handles if w != original_window]
                                    for window in open_windows:
                                        if exit_requested: break
                                        driver.switch_to.window(window)
                                        # --- Pass csv_writer and firm_name ---
                                        extract_ranking_data(driver, wait, ranking_location, region_name, all_scraped_data[region_name], csv_writer, firm_name_to_process)
                                        driver.close()
                                    if exit_requested: break
                                    
                                    driver.switch_to.window(original_window)
                                    if not exit_requested:
                                        write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", f"Finished Batch {batch_num + 1}/{num_batches}")
                                
                                if not exit_requested:
                                    log_queue.put( ("\t✅ All batches processed.", "success") )
                                    write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", "Finished processing all batches.")

                        if not exit_requested:
                            # --- UPDATED: Pass folder path to save function ---
                            save_regional_data(region_folder_path, all_scraped_data[region_name], region_name, firm_name_to_process, len(all_scraped_data[region_name]))
                            write_simple_csv_log(csv_writer, firm_name_to_process, "INFO", "--- Finished scrape for firm ---")
                    
                    # --- UPDATED: Specific TimeoutException handling ---
                    except TimeoutException:
                        # This catches the timeout from waiting for the H1 tag
                        log_queue.put( (f"   ⚠️ Timed out on firm page for '{firm_name_to_process}'. Skipping.", "warning") )
                        write_simple_csv_log(csv_writer, firm_name_to_process, "WARN", "Not a firm page (or timed out). Skipping.")
                    except Exception as e:
                        # This catches other errors, like the "Not a firm page" I raise manually
                        if "Not a firm page" in str(e):
                            pass # Already logged this error
                        else:
                            log_queue.put( (f" ❌ ERROR scraping {firm_name_to_process}: {e}", "error") )
                            error_msg = str(e).splitlines()[0]
                            write_simple_csv_log(csv_writer, firm_name_to_process, "CRITICAL", f"Critical error during firm scrape: {error_msg}")
                    
                    if exit_requested: break
                    
                    log_queue.put( (f"   ✅ Finished. Navigating back...", "success") )
                    try:
                        driver.back()
                        time.sleep(random.uniform(3, 5))
                        handle_cookies_if_present(driver)
                    except Exception as e:
                        log_queue.put( (f"   ❌ ERROR navigating back. Restarting browser. Error: {e}", "error") )
                        error_msg = str(e).splitlines()[0]
                        write_simple_csv_log(csv_writer, firm_name_to_process, "CRITICAL", f"Error on driver.back(). Restarting. Error: {error_msg}")
                        # Force a restart
                        session_firm_count = 14 # Will trigger restart on next loop
                    
                    session_firm_count += 1
                
                if exit_requested: break
            
            except Exception as region_e:
                 log_queue.put( (f"\n❌ CRITICAL ERROR IN REGION {region_name}: {region_e}", "error") )
                 if csv_writer: # Try to log the error to the CSV if possible
                    error_msg = str(region_e).splitlines()[0]
                    write_simple_csv_log(csv_writer, "N/A", "CRITICAL", f"Unhandled error processing region: {error_msg}")
            finally:
                # --- NEW: This block now runs at the end of *each region* ---
                
                # --- NEW: Quit the driver at the end of every region ---
                if driver:
                    driver.quit()
                    log_queue.put( (f"   Browser for {region_name} closed.", "info") )
                    driver = None # Set to None
                
                if csv_file and csv_writer:
                    # --- ADDED: Log final total for region ---
                    total_rankings = 0
                    if region_name in all_scraped_data: # Check if key exists
                        total_rankings = len(all_scraped_data[region_name])
                    log_queue.put( (f"   📊 Final total for {region_name}: {total_rankings} rankings.", "info") )
                    write_simple_csv_log(csv_writer, "SCRIPT_STATUS", "INFO", f"Region finished. Final total rankings: {total_rankings}")
                    # --- END ADDED ---
                    csv_file.close()
                    log_queue.put( (f"💾 Closed log for {region_name}.", "info") )
    
    except Exception as e:
        log_queue.put( (f"\n❌ A CRITICAL, UNHANDLED ERROR OCCURRED: {e}", "error") )
    finally:
        # This is the main shutdown block. It runs on exit, error, or completion.
        log_queue.put( ("\n" + "═"*20 + "\n 🏁 SCRAPING COMPLETE OR HALTED 🏁\n" + "═"*20, "header") )
        
        # --- CRITICAL: SAVE ALL DATA ---
        log_queue.put( ("   Saving all collected data one last time...", "info") )
        # --- UPDATED: Loop, create folders, and save ---
        for region, data in all_scraped_data.items():
            if data:
                # Ensure folder exists for final save
                safe_name_final = re.sub(r'[\\/*?:"<>|]', "", region).replace(' ', '_')
                folder_path_final = os.path.join(base_output_dir, safe_name_final)
                os.makedirs(folder_path_final, exist_ok=True)
                # Pass folder path to save function
                save_regional_data(folder_path_final, data, region, "Final Save", len(data))
        
        if driver: # If a driver is still open (e.g., from an error), close it.
            driver.quit()
        log_queue.put( ("   All files saved. Browser closed.", "info") )
        
        # --- Signal to GUI that thread is done ---
        try:
            start_button.config(state='normal', text="Start Scraping")
        except tk.TclError:
            pass # GUI is already gone
            
        scraper_thread_running = False
        # If exit was requested, the GUI's process_log_queue will now see the flag and exit.
        if exit_requested:
            log_queue.put( ("   Exiting application now.", "info") )
        else:
            log_queue.put( ("   Ready to start a new job.", "info") )
            

if __name__ == '__main__':
    root = tk.Tk()
    app = ScraperApp(root)
    root.mainloop()

