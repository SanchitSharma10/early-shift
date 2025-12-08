import os
import requests
from dotenv import load_dotenv

load_dotenv('.env')
API_KEY = os.getenv('YOUTUBE_API_KEY')
if not API_KEY:
    raise SystemExit('Missing YOUTUBE_API_KEY')

query_names = {
    'GamingWithKev': 'gamingwithkev',
    'Leah Ashe': 'LeahAshe',
    'CookieSwirlC': 'CookieSwirlC',
    'InquisitorMaster': 'InquisitorMaster',
    'ItsFunneh': 'ItsFunneh',
    'FGTeeV': 'FGTeeV',
    'Flamingo': 'Flamingo',
    'Sketch': 'Sketch',
    'Denis': 'Denis',
    'Thinknoodles': 'Thinknoodles'
}

ids = {}
for name, handle in query_names.items():
    url = 'https://www.googleapis.com/youtube/v3/search'
    params = {
        'part': 'snippet',
        'q': handle,
        'type': 'channel',
        'maxResults': 1,
        'key': API_KEY,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    items = resp.json().get('items', [])
    if not items:
        print(f'No results for {name}')
        continue
    channel_id = items[0]['id']['channelId']
    ids[name] = channel_id
    print(f'{name}: {channel_id}')

print('\nCollected IDs:')
print(ids)
