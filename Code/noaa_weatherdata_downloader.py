# =============================================================================
# --- NOAA Integrated Surface Database (ISD) Downloader ---
#
# Author: Sajjad Uddin Mahmud
#         School of Electrical Engineering and Computer Science
#         Washington State University, Pullman, WA, USA
#
# File: noaa_weatherdata_downloader.py
#       - this code will download the NOAA ISD data (https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) for a given year / list of years.
#       - first, it will download all the weather stations .csv file of each year from the website and save in "{year} Weather Station Data - All" folder.
#       - then, it will separate the USA based weather stations by checking each weather station's name and move them state-wise in "{year} Weather Station Data - USA" folder.
#       - once it moves all the usa based stations, it will remain the "{year} Weather Station Data - All" to "{year} Weather Station Data - Rest".
# 
# Note:    
#       - some year may take longer time due to download interruption, in that case this script will keep trying (max_retries = 10) to download.
#       - if the user have the .tar.zip file downloaded already, it can be pasted in any folder within the main folder. In that case, the code will skip downloading again and use the existing downloaded file to proceed. 
# =============================================================================


# =============================================================================
# Required Packages
# =============================================================================
import os
import pandas as pd
import shutil
import time

import requests
from tqdm import tqdm
import tarfile
import re

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import warnings
warnings.filterwarnings('ignore')


# =============================================================================
# User Input
# =============================================================================

year_list = [2024] # Give a list of years; eg. [2024] for single year, or [2023, 2024, 2020] for different years, or list(range(2010,2025)) for year from 2010 to 2024 (2025 is exclusive)


# =============================================================================
# Accessing Working Folders
# =============================================================================

# Get the current script folder path
script_filepath = os.path.abspath(__file__)
script_folderpath = os.path.dirname(script_filepath) 

# Get the master folder
parent_folderpath = os.path.dirname(script_folderpath)

# Get weather data folder path
weather_data_folderpath = os.path.join(parent_folderpath, 'Output - NOAA ISD Data')
os.makedirs(weather_data_folderpath, exist_ok = True) # Create folder if does not exist


# =============================================================================
# Functions
# =============================================================================

def find_existing_tar_file(year):
    """
    Searches for an existing {year}.tar.gz file in the script's directory and subdirectories.
    Returns the file path if found, otherwise returns None.
    """
    for root, _, files in os.walk(parent_folderpath):  # Search in all folders
        for file in files:
            if file == f"{year}.tar.gz":
                return os.path.join(root, file)  # Return the full path of the found file
    return None  # Return None if no file is found


def weatherdata_download_and_save(year, save_dir, max_retries = 10, retry_delay = 30):
    """
    Downloads a .tar.gz file from the NOAA website for the specified year and saves it in the specified directory.
    Implements retries, resuming downloads, and handles network failures.

    Parameters:
    - year: The year for which the file is to be downloaded.
    - save_dir: The directory where the downloaded file should be saved.
    - max_retries: Number of times to retry in case of failure (default=10).
    - retry_delay: Delay (seconds) before retrying a failed download.

    Returns:
    - file_path: The path to the saved .tar.gz file, or None if download fails after retries.
    """

    base_url = "https://www.ncei.noaa.gov/data/global-hourly/archive/csv/"
    file_name = f"{year}.tar.gz"
    url = f"{base_url}{file_name}"
    
    # Ensure the save directory exists
    os.makedirs(save_dir, exist_ok=True)
    
    # Define the path where the file will be saved
    file_path = os.path.join(save_dir, file_name)

    # Set up a session with retries
    session = requests.Session()
    retries = Retry(total = max_retries, backoff_factor = 2, status_forcelist = [500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    #Loop: to download the file
    for attempt in range(1, max_retries + 1):

        try:
            # Check if file exists to resume download
            headers = {}
            if os.path.exists(file_path):
                existing_size = os.path.getsize(file_path)
                headers["Range"] = f"bytes={existing_size}-"
            else:
                existing_size = 0

            response = session.get(url, headers = headers, stream = True, timeout = 60)
            
            if response.status_code in [200, 206]:  # 206 means partial content (resumed download)
                total_size = int(response.headers.get("content-length", 0)) + existing_size
                block_size = 1024 * 1024  # 1 MB chunks

                print(f"\nDownloading: {file_name} (Resuming from {existing_size} bytes)")

                with open(file_path, "ab") as file, tqdm(
                    desc = file_name,
                    total = total_size,
                    initial = existing_size,
                    unit = "iB",
                    unit_scale = True,
                    unit_divisor = 1024,
                ) as bar:
                    for data in response.iter_content(block_size):
                        file.write(data)
                        bar.update(len(data))

                print(f"{file_name} downloaded and saved at {file_path}")
                return file_path  # Return file path on success

            else:
                print(f"Failed to download {file_name}. HTTP Status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Download failed for {file_name}. Error: {e}")

        if attempt < max_retries:
            print(f"Retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})")
            time.sleep(retry_delay)
        else:
            print(f"Maximum retries reached. Skipping {file_name}.")
            return None  # Return None if all retries fail

# Extract data from a .tar file
def weatherdata_extract_file(tar_file_path, base_directory):
    """
    Unzips a .tar.gz file to the specified directory and delete the .tar.gz file from base directory after successfully extracted the data.
    
    Parameters:
    - tar_file_path: The path to the .tar.gz file.
    - base_directory: The directory where the main .tar.gz file exists.
    
    Returns:
    - extract_directory: The directory where files were extracted. Directory name format: "<.tar.gz file year> Weather Station Data"
    
    """
    # get the file name
    file_name = os.path.basename(tar_file_path)

    # Remove the .tar.gz extension to get the file year from file name
    file_year = file_name.replace('.tar.gz', '')

    # Set the extraction directory
    extract_directory = os.path.join(base_directory, f"{file_year} Weather Station Data - All")

    # Ensure the extraction directory exists
    os.makedirs(extract_directory, exist_ok = True)
    
    # Extracting the file
    with tarfile.open(tar_file_path, "r:gz") as tar:

        print(f"Extracting data from {tar_file_path}")

        file_list = tar.getmembers()  # get the list of files in the tar archive
        
        with tqdm(total = len(file_list), desc = f"Extracting {os.path.basename(tar_file_path)}") as bar:
            for file in file_list:
                tar.extract(file, extract_directory)
                bar.update(1)
    
    print(f"Files extracted to {extract_directory}")

    # Delete the tar.gz file after extraction
    os.remove(tar_file_path)
    print(f"{file_name} has been deleted the from the base directory")
    
    return extract_directory

# Extract US state abbreviation
def extract_state_abbreviation(input_string):
    """
    Extract the US state abbreviation from the weather station name string. 
    For example: if the name of a weather station is "PORTAGE GLACIER VISITOR CENTER, AK US" in the weather station data
    this script will return "AK"

    Parameters:
    - input_string: A string which may contain on US state abbreviation like this "......, AK US"

    Returns:
    - state_abbreviation: US state abbreviation in the string
        
    """
    
    # Define a regular expression pattern for US state abbreviations
    state_pattern = re.compile(r'\b([A-Z]{2})\s*,?\s*US\b') 

    # Search for the pattern in the input string
    match = state_pattern.search(input_string)

    # Check for a match
    if match:
         
         # Wxtract the state abbreviation and return
         state_abbreviation = match.group(1)
         return state_abbreviation

    # In case of no state, return None    
    else:
         return None

# Filter USA weather stations and sort into state wise folder
def usa_weatherstation_filter(year, raw_data_directory, output_folderpath):
    """
    Filters USA-based weather stations from raw weather data files, sorts them into state-wise folders,
    and creates a summary CSV file.

    Parameters:
    year (int): The year of the weather data.
    raw_data_directory (str): Path to the directory containing raw weather data CSV files.
    output_folderpath (str): Path to the directory where filtered data should be saved.

    Output:
    - Filtered CSV files sorted into state-wise folders under `output_folderpath/{year} Weather Station Data - USA/{state}`.
    - A summary file `{year}_USA Weather Stations.csv` saved in `output_folderpath`.
    - Remaining non-USA data folder renamed to `{year} Weather Station Data - Rest`.
    
    Raises:
    - ValueError if no USA weather stations are found (fixed in the updated version).
    """

    # Get all .csv files from raw weather data folder (all stations)
    csv_files = [filename for filename in os.listdir(raw_data_directory) if filename.endswith('.csv')]

    # Summary dataframe
    summary_df = []
    usa_files = set()  # Track files that are USA-based

    # Access to all csv files
    for file in range(len(csv_files)):
            
            # Get csv file path
            csv_filepath = os.path.join(raw_data_directory,csv_files[file])
            print(csv_files[file])

            # Read csv
            csv_df = pd.read_csv(csv_filepath, low_memory=False)

            # Get the station name
            station_name = csv_df.loc[0, 'NAME']

            # Check if there is nothing in the NAME column
            if pd.notna(station_name) == True:

                # Check if the country is USA
                country_name = station_name[-2:]

                if country_name == 'US':
                
                    # Get the state name
                    state_name = extract_state_abbreviation(station_name)
                    
                    # Check if state name is not in the name
                    if state_name is not None:
                    
                        print(state_name)

                        # Create output folder for each state
                        state_folderpath = os.path.join(output_folderpath, f"{year} Weather Station Data - USA", state_name)
                        os.makedirs(state_folderpath, exist_ok=True)

                        destination_path = os.path.join(state_folderpath, os.path.basename(csv_filepath))

                        # Move file instead of copy (saves memory)
                        shutil.move(csv_filepath, destination_path)
                        
                        # Track this file as moved
                        usa_files.add(file)

                        # Create summary excel
                        summary_df.append(pd.DataFrame({'CSV': [csv_files[file]], 'State': [state_name]}))

    # If USA-based weather stations are found, save the summary CSV
    if summary_df:
        index_df = pd.concat(summary_df, ignore_index = True)
        index_df_filename = str(year) + ' - USA Weather Stations.csv'
        index_df.to_csv(os.path.join(output_folderpath, index_df_filename), index = False)
        print(f"Filtering USA weather stations for year {year} is completed")

    else:
        # If no USA weather stations were found, create a text file instead
        no_data_filepath = os.path.join(output_folderpath, f"{year}_No_USA_Weather_Stations.txt")
        with open(no_data_filepath, 'w') as f:
            f.write(f"No USA-based weather stations found for the year {year}.")
        print(f"No USA-based weather stations found for {year}. A text file has been created.")
        
    # Rename the remaining folder to "Rest"
    rest_directory = os.path.join(output_folderpath, f"{year} Weather Station Data - Rest")
    if os.path.exists(raw_data_directory) and not os.path.exists(rest_directory):
        os.rename(raw_data_directory, rest_directory)
        print(f"Renamed '{raw_data_directory}' to '{rest_directory}'")


# =============================================================================
# Main
# =============================================================================

for year in year_list:
    
    # Create the output folder path
    output_folderpath = os.path.join(weather_data_folderpath, str(year))
    os.makedirs(output_folderpath, exist_ok = True)

    # Search for the {year}.tar.gz file in all folders
    downloaded_zipfile_path = find_existing_tar_file(year)

    if downloaded_zipfile_path:
        print(f"Found existing {year}.tar.gz at {downloaded_zipfile_path}. Skipping download.")
    else:
        # Keep retrying download until successful
        while True:
            downloaded_zipfile_path = weatherdata_download_and_save(year, output_folderpath)

            if downloaded_zipfile_path is not None:
                break  # Exit loop when download is successful
            else:
                print(f"Retrying full download for {year} in 1 minute...")
                time.sleep(60)  # Wait before retrying

    # Extract from downloaded tar (zip) file
    extract_directory = weatherdata_extract_file(downloaded_zipfile_path, output_folderpath)

    # Filter by USA weather stations
    usa_weatherstation_filter(year, extract_directory, output_folderpath)



    
    