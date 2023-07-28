import requests

# URL to scrape
url = 'https://archiveofourown.org/tags/Sharlock/works'

def get_redirect_url(url):
    response = requests.head(url, allow_redirects=False)
    if 'Location' in response.headers:
        return response.headers['Location']
    else:
        return None

# Make the request
for i in range(1_000):
    print(get_redirect_url(url))