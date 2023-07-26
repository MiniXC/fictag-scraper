import requests
import os
import time
import lco
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

SCRAPE_OPS_KEY = os.getenv("SCRAPE_OPS_KEY")

last_scrape_time = None

def scrape_url(url, try_count=0):
    global last_scrape_time
    now = time.time()
    if lco["use_proxy"]:
        request_dict = {
            "url": 'https://proxy.scrapeops.io/v1/',
            "params": urlencode({
                'api_key': SCRAPE_OPS_KEY,
                'url': url,
                'country': lco["proxy_country"],
            })
        }
        interval = lco["proxy_interval"]
    else:
        request_dict = {
            "url": url,
            # believable user agent for scraping
            "headers": {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
            }
        }
        interval = lco["regular_interval"]
    if last_scrape_time is None:
        last_scrape_time = now
        # 2 minute timeout
        response = requests.get(**request_dict, timeout=200)
    elif now - last_scrape_time > interval:
        last_scrape_time = now
        response = requests.get(**request_dict, timeout=200)
    else:
        time.sleep(interval)
        if try_count > 0:
            print(f"sleeping (after {try_count} tries)")
        return scrape_url(url, try_count=try_count+1)
    return response