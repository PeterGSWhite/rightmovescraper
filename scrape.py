import requests
import random
import json
import multiprocessing as mp
import time
from config import county_ids as county_ids_list

county_ids_list = county_ids_list[:2]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
    'Cookie': 'TS01826437=012f990cd3bad3a01b9af0d7a86bcb9f072609bfb7679fda908aff01648fbf82ed92a89e3572ff6eae20143d6b93ff137b13a02abe; TS01a07bd2=012f990cd3bad3a01b9af0d7a86bcb9f072609bfb7679fda908aff01648fbf82ed92a89e3572ff6eae20143d6b93ff137b13a02abe; permuserid=2112087QXSLOT6B3PD3GISUK4LDXA3JO; TS01ec61d1=012f990cd3bad3a01b9af0d7a86bcb9f072609bfb7679fda908aff01648fbf82ed92a89e3572ff6eae20143d6b93ff137b13a02abe; cdUserType=none; rmsessionid=1316d4db-8b1f-41d1-a3ba-149a2e73e9cb; lastSearchChannel=RES_BUY; beta_optin=N:57:-1; RM_Register=C; permuserid=2112087QXSLOT6B3PD3GISUK4LDXA3JO; JSESSIONID=BF4075B8C10520FD8CFCB04E2DC43AF2; svr=3103; lastViewedFeaturedProperties=82847520|116802806|112557902|117327095|82884780; TS019c0ed0=012f990cd394ef0cf1bc20879f3a1eed03ba7bbafae3fc2f6358e8952f07c25d954e0a745ea9056aa5ccce771d864893f396343099',
}

perPage = 24



def scrape_county(county_id):
    print('scraping county', county_id)
    last = 1000000
    index = 0
    while index < last:
        if index > 4:
            break
        api_url = f'https://www.rightmove.co.uk/api/_search?locationIdentifier=REGION%5E{county_id}&numberOfPropertiesPerPage={perPage}&radius=0.0&sortType=2&index={index}&viewType=LIST&channel=BUY&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false'
        response = requests.get(api_url, headers=headers)
        data = json.loads(response.text)
        last = int(data['pagination']['last'])
        
        for property in data['properties']:
            yield property
            
        time.sleep(0.5 + random.random())
        
        index += perPage

def scrape_counties(q_in, q_out):
    while True:
        county_id = q_in.get()
        if county_id is None:
            break
        for property in scrape_county(county_id):
            q_out.put(property)

def parse_property(property):
    return {
        'id': property['id'],
        'bedrooms': property['bedrooms'],
        'bathrooms': property['bathrooms'],
        'price': property['price']['amount'],
        'display_address': property['displayAddress'],
        'location': property['location'],
        'property_type': property['propertySubType'],
        'listing_update': property['listingUpdate'],
    }

def post_properties(q_in):
    while True:
        property = q_in.get()
        if property is None:
            break
        property = parse_property(property)
        print('posting property', json.dumps(property, indent=2))

def wait(procs):
    alive_count = len(procs)
    while alive_count:
        alive_count = 0
        for p in procs:
            if p.is_alive():
                p.join(timeout=0.1)
                print('q1 size', county_ids.qsize(), 'q2 size', properties.qsize())
                alive_count += 1

if __name__ == "__main__":
    county_ids = mp.Queue()
    properties = mp.Queue()
    scrape_processes = [mp.Process(target=scrape_counties, args=(county_ids, properties,))
            for i in range(5)]
    post_processes = [mp.Process(target=post_properties, args=(properties,))
            for i in range(3)]
    for p in scrape_processes + post_processes:
        p.start()

    for county_id in county_ids_list:
        county_ids.put(county_id)
    for p in scrape_processes: # terminate scraper processes
        county_ids.put(None)

    wait(scrape_processes)
    for p in post_processes: # terminate post processes
        properties.put(None)
    wait(post_processes)