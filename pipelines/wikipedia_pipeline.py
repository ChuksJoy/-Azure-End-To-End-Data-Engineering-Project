import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


NO_IMAGE = 'upload.wikimedia.org'

def get_wikipedia_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def get_wikipedia_data(html):
    if not html: return []
    soup = BeautifulSoup(html, 'html.parser')
    # Find all wikitables and look for the one containing 'Stadium'
    tables = soup.find_all("table", {"class": "wikitable"})
    for table in tables:
        if "Stadium" in table.text:
            return table.find_all('tr')
    return []

def clean_text(text):
    text = str(text).strip().replace('\n', '')
    for marker in [' ♦', '[', ' (formerly)']:
        if marker in text: text = text.split(marker)[0]
    return text.strip()

def extract_wikipedia_data(**kwargs):
    url = kwargs.get('url')
    html = get_wikipedia_page(url)
    
    if html is None:
        raise ValueError("Wikipedia blocked the request (403).")
        
    rows = get_wikipedia_data(html)
    data = []

    # Iterate through rows, skipping the header
    for i, row in enumerate(rows[1:], start=1):
        # FIX: Find BOTH td and th tags because Rank is often in a <th>
        tds = row.find_all(['td', 'th'])
        
        # Ensure the row has enough columns (Rank, Stadium, Capacity, Region, Country, City, Images, Home Team)
        if len(tds) >= 8:
            try:
                # Column mapping based on the HTML provided:
                # tds[0] = Rank (th)
                # tds[1] = Stadium (td)
                # tds[2] = Capacity (td)
                # tds[3] = Region (td)
                # tds[4] = Country (td)
                # tds[5] = City (td)
                # tds[6] = Images (td)
                # tds[7] = Home Team (td)
                
                img_tag = tds[6].find('img')
                image_url = 'https:' + img_tag.get('src') if img_tag else NO_IMAGE
                
                values = {
                    'rank': clean_text(tds[0].text),
                    'stadium': clean_text(tds[1].text),
                    'capacity': clean_text(tds[2].text).replace(',', ''),
                    'region': clean_text(tds[3].text),
                    'country': clean_text(tds[4].text),
                    'city': clean_text(tds[5].text),
                    'images': image_url,
                    'home_team': clean_text(tds[7].text),
                }
                data.append(values)
            except Exception as e:
                print(f"Error parsing row {i}: {e}")
                continue

    # # Write to CSV
    # if data:
    #     data_df = pd.DataFrame(data)
    #     data_df.to_csv('/opt/airflow/data/wikipedia_stadiums.csv', index=False)
    #     print(f"Successfully wrote {len(data)} rows to CSV.")
    # else:
    #     print("Warning: No data extracted.")

    # Final step: Serialize the list of dictionaries to a JSON string
    json_rows = json.dumps(data) 
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    return "OK"


def get_lat_long(country, city):
    # user_agent is required by Nominatim policy
    geolocator = Nominatim(user_agent='IvoryStadiumexplore2025')
    try:
        location = geolocator.geocode(f'{city}, {country}', timeout=10)
        
        # 3. Add a small sleep to respect Nominatim's 1-request-per-second policy
        import time
        time.sleep(1) 
        
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Geocoding error: {e}")
    return None


# 2. Optimized Geocoding & Transformation Task
def transform_wikipedia_data(**kwargs):
    from datetime import datetime
    # 1. Pull data from XCom
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    # 2. Create DataFrame
    stadiums_df = pd.DataFrame(data)
    
    # 3. Apply transformations
    # First attempt: Geocode using Stadium Name
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    
    # Clean images and capacity
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    
    # Capacity clean: handle cases where commas or strings might still exist
    stadiums_df['capacity'] = pd.to_numeric(stadiums_df['capacity'].str.replace(',', ''), errors='coerce').fillna(0).astype(int)

    # 4. Handle duplicates by falling back to City name
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    if not duplicates.empty:
        duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
        stadiums_df.update(duplicates)

    # 5. Push to XCom as JSON
    # Note: Use to_json() without arguments to match standard Airflow serialization
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json(orient='records'))

    return "OK"

#Write wikipedia data to database
def write_wikipedia_data(**kwargs):
    from datetime import datetime
    import pandas as pd
    import json

     # 1. Pull data from XCom
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    # Check if data exists to prevent json.loads(None) errors
    if data is None:
        raise ValueError("No data received from transform_wikipedia_data")
    
    # 2. Parse the data
    data = json.loads(data)
    data = pd.DataFrame(data)

    # 3. Create the filename (This part of your code is correct)
    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    # 4. Write to local storage
    data.to_csv('/opt/airflow/data/' + file_name, index=False)


