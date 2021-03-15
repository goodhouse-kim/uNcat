import http.client, json, time, omdb, csv
from itertools import count

SOUTH_KOREA_COUNTRY_CODE = 348
KOREAN_AUDIO = "korean"

RAPIDAPI_KEY = "get yer own"
RAPIDAPI_HOST ="unogsng.p.rapidapi.com"
OMDB_API_KEY = "get yer own"

TRANSFORMED_CAT_COLUMNS = ['title', 'type', 'year', 'rated', 'released', 'genre', 'director', 'writer', 
        'language', 'country', 'imdb_id', 'imdb_rating', 'imdb_votes', 'production', 'website', 'poster']

def store_countries():
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)

    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
        }

    conn.request("GET", "/countries", headers=headers)

    res = conn.getresponse()
    data = res.read()

    write_json("countries.json", json.loads(data))
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

def get_cat(country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None):
    cat = []
    offset = 0
    total = 0
    # as much as I hate infinite loops on purpose,
    # when we're getting billed by individual api calls, we can get a little hacky in the spirit of frugality
    while True:
        res = empty_search(country_code, audio, offset)
        # print(res)
        if 'results' not in res or len(res['results']) == 0:
            return cat 

        # total is only returned if offset is 0
        if 'total' in res:
            total = res['total']
        else:
            print('total missing from api response')
            
        cat += res['results']

        print("{}: {} collected out of {}".format(country_code, len(cat), total))
        offset = len(cat)

        # we're not fetching that much data that often, but I really hope rapidAPI has proper throttling 
        # since I'm too lazy to verify, I'll just assume that they have some level of throttling
        # and sleep enough to avoid hitting TPS limit
        time.sleep(0.5)

def store_cat(file_name='data.json', country_code=SOUTH_KOREA_COUNTRY_CODE, audio=None):
    print("collecting cat for {}".format(country_code))

    result = get_cat(country_code=country_code, audio=audio)

    write_json(file_name, result)

# Needs country.json
def store_all_cat_per_country(audio=None):
    # load country and fetch all cat + save to file
    with open('country.json') as country_file:
        country_json = json.load(country_file)

        for country in country_json['results']:
            country_code = country['id']
            country_name = country['country'].replace(" ", "")

            store_cat('{}_cat.json'.format(country_name), country_code, audio=audio)

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
        for country in country_json['results']:
            country_name = country['country'].replace(" ", "")
            with open('{}_cat.json'.format(country_name)) as country_cat_json:
                country_cat_list = json.load(country_cat_json)
                for title in country_cat_list:
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

def index_omdb_titles():
    indexed_imdb_tt_omdb_cat = {}
    with open('omdb_title.json') as omdb_titles_file:
        omdb_titles = json.load(omdb_titles_file)
        for omdb_title in omdb_titles:
            indexed_imdb_tt_omdb_cat[omdb_title['imdb_id']] = omdb_title
        
    write_json('indexed_imdb_tt_omdb_cat.json', indexed_imdb_tt_omdb_cat)

def write_to_csv(filename, columns, data):
    with open(filename, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def get_transformed(input_file_name, omdb_cat, filter_country=None):
    transformation = []

    with open(input_file_name) as country_cat_file:
        country_cat = json.load(country_cat_file)
        for title in country_cat:
            if 'imdbid' in title and title['imdbid'] and title['imdbid'] != 'notfound':
                imdb_id = title['imdbid']
                omdb_title = omdb_cat[imdb_id]

                if filter_country is None or filter_country in omdb_title.get('country', 'N/A').lower():
                    row = {
                        'title': omdb_title.get('title', 'N/A'),
                        'type': omdb_title.get('type', 'N/A'),
                        'year': omdb_title.get('year', 'N/A'),
                        'rated': omdb_title.get('rated', 'N/A'),
                        'released': omdb_title.get('released', 'N/A'),
                        'genre': omdb_title.get('genre', 'N/A'),
                        'director': omdb_title.get('director', 'N/A'),
                        'writer': omdb_title.get('writer', 'N/A'),
                        'language': omdb_title.get('language', 'N/A'),
                        'country': omdb_title.get('country', 'N/A'),
                        'imdb_id': omdb_title.get('imdb_id', 'N/A'),
                        'imdb_rating': omdb_title.get('imdb_rating', 'N/A'),
                        'imdb_votes': omdb_title.get('imdb_votes', 'N/A'),
                        'production': omdb_title.get('production', 'N/A'),
                        'website': omdb_title.get('website', 'N/A'),
                        'poster': omdb_title.get('poster', 'N/A')
                    }

                    transformation.append(row) 

    return transformation

def produce_transformed_cat(input_file_name='SouthKorea_catalog.json', output_file_name='netflix_korea_titles.csv'):
    transformation = []
    # index omdb titles
    with open('indexed_imdb_tt_omdb_catalog.json') as omdb_cat_file:
        omdb_cat = json.load(omdb_cat_file)
        
        transformation = get_transformed(input_file_name, omdb_cat)
                
    write_to_csv(output_file_name, TRANSFORMED_CAT_COLUMNS, transformation)

# Takes all the json file produced from above and produces a neat csv with metadata including country data
def produce_worldwide_per_country_reduced_cat(filter_country="korea"):
    # index omdb titles
    with open('indexed_imdb_tt_omdb_catalog.json') as omdb_titles_file:

        files_to_reduce = []
        omdb_titles = json.load(omdb_titles_file)

        with open('country.json') as country_file:
            country_json = json.load(country_file)
            for country in country_json['results']:
                files_to_reduce.append('{}_catalog.json'.format(country['country'].replace(" ", "")))

        print(files_to_reduce)

        for cat_file_name in files_to_reduce:
            transformation = get_transformed(cat_file_name, omdb_titles, filter_country=filter_country)
            write_to_csv("transformed_{}".format(cat_file_name).replace(".json", ".csv"), TRANSFORMED_CAT_COLUMNS, transformation)
