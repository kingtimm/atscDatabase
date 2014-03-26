import csv
import json
import aiohttp
import asyncio
from bs4 import BeautifulSoup


def load_csv_data():
    with open('raw.csv', 'r') as f:
        data = csv.reader(f)
        dictionary = {}
        for item in data:
            row = {item[0]: item[1]}
            dictionary.update(row)
        return dictionary


@asyncio.coroutine
def get_lat_lon_from_zip(zip_code):
    params = dict(sensor='false', address=zip_code)
    baseurl = "http://maps.googleapis.com/maps/api/geocode/json"

    result = yield from aiohttp.request(method='GET', url=baseurl,
                                        params=params)

    result = yield from result.read_and_close()

    result = json.loads(result.decode('utf-8'))

    if result['status'] == "OK":
        result = result['results'][0]['geometry']['location']
        return result['lat'], result['lng']
    else:
        print("ERR {0}".format(result))
        return 0, 0


@asyncio.coroutine
def get_dtv_db_response(lat, lon):
    params = dict(expert=1, startpoint="{0},{1}".format(lat, lon))
    baseurl = "http://transition.fcc.gov/cgi-bin/maps/coverage.pl"

    response = yield from aiohttp.request(method='GET', url=baseurl,
                                          params=params)

    response = yield from response.read_and_close()

    results = []

    soup = BeautifulSoup(response.decode('utf-8'))
    table = soup.find(summary="Search Result Table")
    for item in table.tbody.find_all("tr"):
        if item.get('class'):
            if item.get('class')[0] in ['strength1', 'strength2', 'strength3',
                                        'strength4']:
                result = {}
                result['power'] = item.get('class')[0]
                result['callsign'] = item.td.td.span.get_text()
                result['aff'] = item.td.td.td.get_text().split('\n')[0]
                result['ch'] = item.td.td.td.td.get_text().split(' ')[0]
                results.append(result)
    return results


@asyncio.coroutine
def get_local_atsc(zip_code, results, semaphore):
    with (yield from semaphore):
        lat, lng = yield from get_lat_lon_from_zip(zip_code)
        yield from asyncio.sleep(1)  # so as not to hammer the google api

    results[zip_code] = yield from get_dtv_db_response(lat, lng)


def build_tasks(data, results, lock):
    return [asyncio.Task(get_local_atsc(data[row], results, lock)) for row in
            data.keys()]


if __name__ == "__main__":
    data = load_csv_data()
    results = {}
    semaphore = asyncio.Semaphore(3)  # so as not to hammer the google api

    print('Starting')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.wait(build_tasks(data, results, semaphore)))

    print('Complete')

    with open("results.json", "w") as f:
        f.write(json.dumps(results, sort_keys=True, indent=4,
                           separators=(',', ': ')))