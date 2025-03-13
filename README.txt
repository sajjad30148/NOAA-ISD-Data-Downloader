# =============================================================================
# NOAA Integrated Surface Database (ISD) Downloader
# =============================================================================
Author: Sajjad Uddin Mahmud  
School of Electrical Engineering and Computer Science  
Washington State University, Pullman, WA, USA  

## Description
This script downloads NOAA ISD (Integrated Surface Database) data for a given year or list of years.
- It retrieves `.tar.gz` files from the NOAA archive: https://www.ncei.noaa.gov/data/global-hourly/archive/csv/
- Downloads the file in "{year} Weather Station Data - All" inside the "NOAA ISD Data" folder.
- Extracts the `.tar.gz` file and filters only USA-based weather stations.
- Sorts the USA station files into state-wise folders: "{year} Weather Station Data - USA/{State}".
- The script will automatically retry failed downloads.
- If the .tar.gz file is already partially downloaded, it will resume from where it stopped.


- if the user have the .tar.zip file downloaded already, it can be pasted in any folder within the main folder. 
In that case, the code will skip downloading again and use the existing downloaded file to proceed. 

---

## How to Use
- Install Required Packages: pip install pandas requests tqdm urllib3
- Run the script inside "Code" folder: noaa_weatherdata_downloader.py
- User input: User will only give the desired year in year_list
  Example:
	year_list = [2000]  # Download data for 2000
	or,
	year_list = [1990, 1995, 2000]  # Download multiple years
	or,
	year_list = list(range(1990, 2025))  # Download 1990 to 2024
