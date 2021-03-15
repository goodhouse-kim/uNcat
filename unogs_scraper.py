import http.client, json, time, omdb

SOUTH_KOREA_COUNTRY_CODE = 348
KOREAN_AUDIO = "korean"
RAPIDAPI_KEY = "get yer own"
RAPIDAPI_HOST ="unogsng.p.rapidapi.com"
OMDB_API_KEY = "get yer own"


def print_countries():
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)

    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
        }

    conn.request("GET", "/countries", headers=headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))

def empty_search(country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None, offset=0, limit=100):
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)

    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
        }
    query = get_search_query(country_code, audio, offset, limit)
    print(query)
    print("API CALL")
    conn.request("GET", query, headers=headers)

    res = conn.getresponse()
    data = res.read()

    # print(data.decode("utf-8"))

    return json.loads(data)

def get_search_query(country_code=None, audio=None, offset=0, limit=100):
    query = "/search?start_year=1950&end_year=2021&orderby=rating&offset={offset}&limit={limit}".format(offset=offset, limit=limit)

    if country_code:
        query += ("&countrylist=" + str(country_code))

    if audio:
        query += ("&audio=" + audio)
    
    return query

# UNUSED
def get_total_count(country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None, offset=0):
    res = empty_search(country_code, audio, offset, limit=1) # we only want the size of the catalog

    # unchecked. can blow up
    return res['total']

def get_catalog(country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None):
    catalog = []
    offset = 0
    total = 0
    # as much as I hate infinite loops on purpose,
    # when we're getting billed by individual api calls, we can get a little hacky in the spirit of frugality
    while True:
        res = empty_search(country_code, audio, offset)
        # print(res)
        if 'results' not in res or len(res['results']) == 0:
            return catalog 

        # total is only returned if offset is 0
        if 'total' in res:
            total = res['total']
        else:
            print('total missing from api response')
            
        catalog += res['results']

        print("{}: {} collected out of {}".format(country_code, len(catalog), total))
        offset = len(catalog)

        # we're not fetching that much data that often, but I really hope rapidAPI has proper throttling 
        # since I'm too lazy to verify, I'll just assume that they have some level of throttling
        # and sleep enough to avoid hitting TPS limit
        time.sleep(0.5)

def store_catalog(file_name='data.json', country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None):
    print("collecting catalog for {}".format(country_code))

    result = get_catalog(country_code=country_code, audio=audio)

    write_json(file_name, result)

# Needs country.json
def store_all_catalog_per_country(audio=None):
    # load country and fetch all catalog + save to file
    with open('country.json') as country_file:
        country_json = json.load(country_file)

        for country in country_json['countries']:
            country_code = country['id']
            country_name = country['country'].replace(" ", "")

            store_catalog('{}_catalog.json'.format(country_name), country_code, audio=audio)

# Needs country.json
def imdb_id_reducer():
    class SetEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(self, obj)

    imdb_ids = set()

    with open('country.json') as country_file:
        country_json = json.load(country_file)
        for country in country_json['countries']:
            country_name = country['country'].replace(" ", "")
            with open('{}_catalog.json'.format(country_name)) as country_catalog_json:
                country_catalog_list = json.load(country_catalog_json)
                for title in country_catalog_list:
                    if 'imdbid' in title and title['imdbid']:
                        imdb_ids.add(title['imdbid'])

    write_json('imdb_id.json', imdb_ids, SetEncoder)

def store_all_imdb_metadata():
    omdb_client = omdb.OMDBClient(apikey=OMDB_API_KEY)
    skipped_ids = []
    omdb_titles = []
    with open('imdb_id.json') as imdb_ids:
        imdb_id_list = json.load(imdb_ids)
        counter = 0

        for imdb_id in imdb_id_list:
            print('https://www.imdb.com/title/{}/'.format(imdb_id))

            try:
                result = omdb_client.get(imdbid=imdb_id)

                if not result:
                    raise RuntimeError("omdb fucked up")

                result['imdb_id'] = imdb_id # in case of empty object? idk if this ever happens
                omdb_titles.append(result)

                # I don't want to hammer omdb too much in case of failure...so we're backing up
                # yeah I'm that lazy
                if counter % 1000 == 0:
                    write_json('omdb_title.json', omdb_titles)

                counter += 1
            except: #OMDBFuckedUpException
                skipped_ids.append(imdb_id)
            
            time.sleep(0.1)

    write_json('omdb_title.json', omdb_titles)
    print("skipped ids:")
    print(skipped_ids)

def write_json(file_name, content, cls=None):
    with open(file_name, 'w') as f:
        json.dump(content, f, ensure_ascii=False, indent=4, cls=cls)


store_all_imdb_metadata()