"""
    Game Library Tools
    IGDB REST API client

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import json
import os
import time
import datetime

from gamelibtools.datatable import DataTable
from gamelibtools.util import *


class IgdbClient:
    """ IGDB REST API client """

    def __init__(self):
        """ Class constructor """
        self.hostname_auth = 'https://id.twitch.tv/oauth2/token'
        self.hostname_api = 'https://api.igdb.com/v4'
        self.hostname_img = 'https://images.igdb.com/image/upload'
        self.clientid = ''
        self.clientsecret = ''
        self.accesstoken = ''
        self.reqlimitms = 250
        self.lastreqtime = 0
        self.data_dir = 'data'
        self.img_dir = 'data/images'
        self.screenshot_dir = 'data/screenshots'
        self.covers_dir = 'data/covers'
        self.artwork_dir = 'data/artwork'
        self.gamecards_dir = 'data/gamecards'
        self.gameindex_dir = 'data/gameindex'
        self.platform_stats_file = 'igdb_platform_stats.csv'
        self.support_data_file = 'igdb_support.json'
        self.support_data = {}
        self.countries = {}
        self.platforms = DataTable(self.data_dir + '/igdb_platforms.csv', ['id', 'name', 'abbreviation', 'alternative_name', 'platform_type', 'platform_family', 'logo', 'logo_url', {'name': 'generation', 'type': 'int'}, 'slug', {'name': 'versions', 'type': 'list'}])
        self.companies = DataTable(self.data_dir + '/igdb_companies.csv', ['id', 'name', 'description', 'start_date', 'logo', 'logo_url', 'country', 'status', 'parent', 'changed_company', 'change_date', 'slug', 'developed', 'published'])
        self.franchises = DataTable(self.data_dir + '/igdb_franchises.csv', ['id', 'name', 'url', { 'name': 'count', 'type': 'int' }, {'name': 'games', 'type': 'list'}])
        self.collections = DataTable(self.data_dir + '/igdb_collections.csv', ['id', 'name', 'url', { 'name': 'count', 'type': 'int' }, {'name': 'games', 'type': 'list'}])
        self.engines = DataTable(self.data_dir + '/igdb_engines.csv', ['id', 'name', 'slug', 'description', 'url', 'logo', 'logo_url', {'name': 'companies', 'type': 'list'}, {'name': 'platforms', 'type': 'list'}])
        self.games_manifest = DataTable(self.data_dir + '/igdb_games_dict.csv', ['id', 'name', 'game_status', 'game_type', 'slug', {'name': 'platforms', 'type': 'list'}, {'name': 'alternative_names', 'type': 'list'},
                                                                                 {'name': 'genres', 'type': 'list'}, {'name': 'developers', 'type': 'list'}, {'name': 'publishers', 'type': 'list'}, {'name': 'release_dates', 'type': 'list'},
                                                                                 {'name': 'metascore', 'type': 'int'}, {'name': 'rating', 'type': 'int'}])
        self.games_plaforms_index = {}
        self.schema_images = ['id', 'name', 'image_id', 'url', {'name': 'width', 'type': 'int'}, {'name': 'height', 'type': 'int'}]
        self.schema_games = []
        self.schema_platform_index = ['id', 'name', 'year', 'game_type', 'release_dates', 'genres', 'metascore', 'rating']
        self.load_games_schema()

        # TODO: Optimize - Fetch image, ref entry, cleanup parse game

    def load(self):
        """ Load authentication data """
        authobj = json.load(open('config/igdbauth.json'))
        if not authobj:
            return
        if 'clientid' in authobj:
            self.clientid = authobj['clientid']
        if 'clientsecret' in authobj:
            self.clientsecret = authobj['clientsecret']

    def auth(self):
        """ Authenticate with the remote server """
        response = requests.post(self.hostname_auth, { 'client_id': self.clientid, 'client_secret': self.clientsecret, 'grant_type': 'client_credentials' })
        if response is None:
            return
        respobj = response.json()
        if respobj is None or 'access_token' not in respobj:
            return
        self.accesstoken = respobj['access_token']

    def load_games_schema(self):
        """ Load games schema """
        self.schema_games = [
            'id',
            'name',
            {'name': 'alternative_names', 'type': 'list'},
            'slug',
            'summary',
            'storyline',
            {'name': 'genres', 'type': 'list'},
            'first_release_date',
            {'name': 'release_dates', 'type': 'list'},
            {'name': 'developers', 'type': 'list'},
            {'name': 'publishers', 'type': 'list'},
            'game_status',
            'game_type',
            {'name': 'aggregated_rating', 'type': 'float'},
            {'name': 'aggregated_rating_count', 'type': 'int'},
            {'name': 'rating', 'type': 'float'},
            {'name': 'rating_count', 'type': 'int'},
            {'name': 'total_rating', 'type': 'float'},
            {'name': 'total_rating_count', 'type': 'int'},
            {'name': 'game_modes', 'type': 'list'},
            {'name': 'languages', 'type': 'list'},
            {'name': 'localizations', 'type': 'list'},
            {'name': 'keywords', 'type': 'list'},
            {'name': 'themes', 'type': 'list'},
            {'name': 'player_perspectives', 'type': 'list'},
            {'name': 'age_ratings', 'type': 'list'},
            {'name': 'multiplayer_modes', 'type': 'list'},
            {'name': 'offlinecoopmax', 'type': 'int'},
            {'name': 'offlinemax', 'type': 'int'},
            {'name': 'onlinecoopmax', 'type': 'int'},
            {'name': 'onlinemax', 'type': 'int'},
            {'name': 'game_engines', 'type': 'list'},
            {'name': 'time_normal', 'type': 'int'},
            {'name': 'time_minimal', 'type': 'float'},
            {'name': 'time_full', 'type': 'float'},
            {'name': 'time_count', 'type': 'float'},
            {'name': 'hypes', 'type': 'int'},
            {'name': 'parent_game', 'type': 'int'},
            {'name': 'similar_games', 'type': 'list'},
            {'name': 'bundles', 'type': 'list'},
            {'name': 'dlcs', 'type': 'list'},
            {'name': 'expanded_games', 'type': 'list'},
            {'name': 'expansions', 'type': 'list'},
            {'name': 'forks', 'type': 'list'},
            {'name': 'ports', 'type': 'list'},
            {'name': 'remakes', 'type': 'list'},
            {'name': 'remasters', 'type': 'list'},
            {'name': 'standalone_expansions', 'type': 'list'},
            {'name': 'collections', 'type': 'list'},
            {'name': 'franchises', 'type': 'list'},
            'version_title',
            {'name': 'version_parent', 'type': 'int'},
            {'name': 'cover', 'type': 'list'},
            {'name': 'screenshots', 'type': 'list'},
            {'name': 'artworks', 'type': 'list'},
            {'name': 'videos', 'type': 'list'},
            {'name': 'websites', 'type': 'list'},
            {'name': 'external_sources', 'type': 'list'},
            'url'
        ]

    def load_support_data(self):
        """
        Load support data from a local file
        If local data is not available it will be imported
        from IGDB and storeed in support data tables
        """
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)
        if not os.path.exists(self.screenshot_dir):
            os.makedirs(self.screenshot_dir)
        if not os.path.exists(self.covers_dir):
            os.makedirs(self.covers_dir)
        if not os.path.exists(self.artwork_dir):
            os.makedirs(self.artwork_dir)
        if not os.path.exists(self.gamecards_dir):
            os.makedirs(self.gamecards_dir)
        if not os.path.exists(self.gameindex_dir):
            os.makedirs(self.gameindex_dir)
        self.countries = json.load(open('config/countries.json'))
        self.load_common_data()
        self.load_companies()
        self.load_platforms()
        self.load_franchises()
        self.load_collections()
        self.load_game_engines()

    def load_common_data(self):
        """ Load common data"""
        Logger.sysmsg(f"Loading support data from IGDB...")
        fpath = self.data_dir + '/' + self.support_data_file
        self.support_data = {}
        if os.path.exists(fpath):
            # Lood local support data
            self.support_data = json.load(open(fpath))
            # Fix keys -> convert from string to int (ID)
            for section in self.support_data:
                newdict = {}
                for key in self.support_data[section]:
                    newdict[int(key)] = self.support_data[section][key]
                self.support_data[section] = newdict
            Logger.log(f"IGDB support tables loaded from {fpath}")
        else:
            # Import data
            Logger.log(f"Fetching platform types...")
            fields = ['id', 'name']
            self.support_data['platform_types'] = self._fetch_map('/platform_types', '', fields)
            for pid in self.support_data['platform_types']:
                self.support_data['platform_types'][pid] = self.support_data['platform_types'][pid].replace("_", " ")

            Logger.log(f"Fetching platform families...")
            fields = ['id', 'name']
            self.support_data['platform_families'] = self._fetch_map('/platform_families', '', fields)

            Logger.log(f"Fetching game types...")
            fields = ['id', 'type']
            self.support_data['game_types'] = self._fetch_map('/game_types', '', fields)

            Logger.log(f"Fetching game statuses...")
            fields = ['id', 'status']
            self.support_data['game_status'] = self._fetch_map('/game_statuses', '', fields)

            Logger.log(f"Fetching game modes...")
            fields = ['id', 'name']
            self.support_data['game_modes'] = self._fetch_map('/game_modes', '', fields)

            Logger.log(f"Fetching regions...")
            fields = ['id', 'name', 'identifier', 'category']
            self.support_data['regions'] = self._fetch_map('/regions', '', fields)

            Logger.log(f"Fetching player perspectives...")
            fields = ['id', 'name']
            self.support_data['player_perspectives'] = self._fetch_map('/player_perspectives', '', fields)

            Logger.log(f"Fetching genres...")
            fields = ['id', 'name']
            self.support_data['genres'] = self._fetch_map('/genres', '', fields)

            Logger.log(f"Fetching themes...")
            fields = ['id', 'name']
            self.support_data['themes'] = self._fetch_map('/themes', '', fields)

            Logger.log(f"Fetching keywords...")
            fields = ['id', 'name']
            self.support_data['keywords'] = self._fetch_map('/keywords', '', fields)

            Logger.log(f"Fetching release date regions...")
            fields = ['id', 'region']
            self.support_data['release_regions'] = self._fetch_map('/release_date_regions', '', fields)
            for x in self.support_data['release_regions']:
                self.support_data['release_regions'][x] = self.support_data['release_regions'][x].replace("_", " ").capitalize()

            Logger.log(f"Fetching release date statuses...")
            fields = ['id', 'name']
            self.support_data['release_status'] = self._fetch_map('/release_date_statuses', '', fields)

            Logger.log(f"Fetching date formats...")
            fields = ['id', 'format']
            self.support_data['date_formats'] = self._fetch_map('/date_formats', '', fields)

            Logger.log(f"Fetching company statuses...")
            fields = ['id', 'name']
            self.support_data['company_status'] = self._fetch_map('/company_statuses', '', fields)
            for x in self.support_data['company_status']:
                self.support_data['company_status'][x] = self.support_data['company_status'][x].capitalize()

            Logger.log(f"Fetching language support types...")
            fields = ['id', 'name']
            self.support_data['language_support_types'] = self._fetch_map('/language_support_types', '', fields)

            Logger.log(f"Fetching languages...")
            fields = ['id', 'name', 'native_name', 'locale']
            self.support_data['languages'] = self._fetch_map('/languages', '', fields)

            Logger.log(f"Fetching collection relation types...")
            fields = ['id', 'name']
            self.support_data['collection_relation_types'] = self._fetch_map('/collection_relation_types', '', fields)

            Logger.log(f"Fetching collection membership types...")
            fields = ['id', 'name']
            self.support_data['collection_membership_types'] = self._fetch_map('/collection_membership_types', '', fields)

            Logger.log(f"Fetching artwork types...")
            fields = ['id', 'name']
            self.support_data['artwork_types'] = self._fetch_map('/artwork_types', '', fields)

            Logger.log(f"Fetching game release formats...")
            fields = ['id', 'format']
            self.support_data['game_release_formats'] = self._fetch_map('/game_release_formats', '', fields)

            Logger.log(f"Fetching external game sources...")
            fields = ['id', 'name']
            self.support_data['external_game_sources'] = self._fetch_map('/external_game_sources', '', fields)

            Logger.log(f"Fetching age rating organizations...")
            fields = ['id', 'name']
            self.support_data['age_rating_organizations'] = self._fetch_map('/age_rating_organizations', '', fields)

            Logger.log(f"Fetching age rating categories...")
            fields = ['id', 'rating', 'organization']
            self.support_data['age_rating_categories'] = self._fetch_map('/age_rating_categories', '', fields)
            for x in self.support_data['age_rating_categories']:
                oid = self.support_data['age_rating_categories'][x]['organization']
                self.support_data['age_rating_categories'][x]['organization'] = self.support_data['age_rating_organizations'][oid]

            Logger.log(f"Fetching age rating content description types...")
            fields = ['id', 'name']
            self.support_data['age_rating_description_types'] = self._fetch_map('/age_rating_content_description_types', '', fields)

            Logger.log(f"Fetching age rating content descriptions...")
            fields = ['id', 'description', 'description_type', 'organization']
            self.support_data['age_rating_descriptions'] = self._fetch_map('/age_rating_content_descriptions_v2', '', fields)
            for x in self.support_data['age_rating_categories']:
                oid = self.support_data['age_rating_descriptions'][x]['organization']
                tid = self.support_data['age_rating_descriptions'][x]['description_type']
                self.support_data['age_rating_descriptions'][x]['organization'] = self.support_data['age_rating_organizations'][oid]
                self.support_data['age_rating_descriptions'][x]['description_type'] = self.support_data['age_rating_description_types'][tid]

            # Write data to a json file
            Logger.log(f"Writing data to a file...")
            with open(fpath, 'w', encoding='utf-8') as f:
                json.dump(self.support_data, f,  indent=4)
            Logger.log(f"IGDB support tables exported to {fpath}")
            Logger.log(f"IGDB support tables loaded")

    def load_companies(self):
        """ Load companies data """
        Logger.sysmsg(f"Loading company data from IGDB...")
        if self.companies.has_file():
            # Lood local platforms data
            self.companies.load()

            # Check logos
            for company in self.companies.data:
                if 'logo' in company and company['logo']:
                    if not os.path.exists(company['logo']):
                        Logger.warning(f"Company {company['name']} logo missing...")
                        download_file(company['logo'], company['logo_url'])
                company['developed'] = int(company['developed'])
                company['published'] = int(company['published'])
            Logger.log(f"IGDB company data loaded from {self.companies.filepath} - {self.companies.count()} entries")
        else:
            # Import data
            cachepath = self.data_dir + '/igdb_cache_company_logos.csv'
            company_logos = DataTable(cachepath, self.schema_images)
            if company_logos.has_file():
                company_logos.load(True)
            else:
                Logger.log(f"Fetching company logos...")
                company_logos.data = self._fetch_table('/company_logos')
                company_logos.index_rows()
                company_logos.save()

            # Define row processor
            def proc_row(y):
                if 'status' in y:
                    if y['status'] in self.support_data['company_status']:
                        y['status'] = self.support_data['company_status'][y['status']]
                    else:
                        Logger.warning(f"Invalid company status: {y['status']}")
                if 'logo' in y:
                    if company_logos.in_index(y['logo']):
                        imginf = company_logos.get_row(y['logo'])
                        imgpath = self.img_dir + '/' + 'company_' + str(y['slug']) + '.jpg'
                        y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                        if os.path.exists(imgpath):
                            y['logo'] = imgpath
                        else:
                            y['logo'] = download_file(imgpath, y['logo_url'])
                    else:
                        Logger.warning(f"Company '{y['name']}' logo reference invalid: {y['logo']}")
                if 'start_date' in y and y['start_date'] > 0:
                    try:
                        dt_object = datetime.datetime.fromtimestamp(y['start_date'])
                        if 'start_date_format' in y:
                            y['start_date'] = self._extract_date(dt_object, self.support_data['date_formats'][y['start_date_format']])
                        else:
                            y['start_date'] = dt_object.strftime("%Y-%m-%d")
                    except Exception as e:
                        Logger.warning(f"Company '{y['name']}' established date parsing failed. {e}")
                if 'change_date' in y and y['change_date'] > 0:
                    try:
                        dt_object = datetime.datetime.fromtimestamp(y['change_date'])
                        if 'change_date_format' in y:
                            y['change_date'] = self._extract_date(dt_object, self.support_data['date_formats'][y['change_date_format']])
                        else:
                            y['change_date'] = dt_object.strftime("%Y-%m-%d")
                    except Exception as e:
                        Logger.warning(f"Company '{y['name']}' changed date parsing failed. {e}")
                if 'country' in y:
                    if str(y['country']) in self.countries:
                        y['country'] = self.countries[str(y['country'])]
                    else:
                        Logger.warning(f"Unknown country code: {y['country']}")
                y['developed'] = len(y['developed']) if 'developed' in y else 0
                y['published'] = len(y['published']) if 'published' in y else 0
                self.companies.add_row(y)

            Logger.log(f"Fetching companies...")
            self.companies.reset()
            self._fetch_table('/companies', sort='name', fp=proc_row)

            # Connect companies
            for cinf in self.companies.data:
                if 'parent' in cinf:
                    pid = cinf['parent']
                    if self.companies.in_index(pid):
                        xinf = self.companies.get_row(pid)
                        cinf['parent'] = { 'id': pid, 'name': xinf['name'] }
                    else:
                        Logger.warning(f"Company '{cinf['name']}' parent reference invalid: {pid}")
                if 'changed_company_id' in cinf:
                    pid = cinf['changed_company_id']
                    if self.companies.in_index(pid):
                        xinf = self.companies.get_row(pid)
                        cinf['changed_company'] = { 'id': pid, 'name': xinf['name'] }
                        cinf.pop('changed_company_id')
                    else:
                        Logger.warning(f"Company '{cinf['name']}' changed company reference invalid: {pid}")

                        # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.companies.save()
            Logger.log(f"IGDB company data loaded")

            # Clear cache
            os.remove(cachepath)

    def load_platforms(self):
        """ Load platform data """
        Logger.sysmsg(f"Loading platform data from IGDB...")
        if self.platforms.has_file():
            # Lood local platforms data
            self.platforms.load()

            # Check platform logos
            for platform in self.platforms.data:
                if 'logo' in platform and platform['logo']:
                    if not os.path.exists(platform['logo']):
                        Logger.warning(f"Platform {platform['name']} logo missing...")
                        download_file(platform['logo'], platform['logo_url'])
            Logger.log(f"IGDB platform data loaded from {self.platforms.filepath} - {self.platforms.count()} entries")
        else:
            # Import data
            Logger.log(f"Fetching platform logos...")
            fields = ['id', 'name', 'image_id', 'url', 'width', 'height', 'animated', 'alpha_channel']
            platform_logos = self._fetch_map('/platform_logos', '', fields)

            Logger.log(f"Fetching platform versions release dates...")
            fields = ['id', 'human', 'release_region']
            platform_version_release_dates = self._fetch_map('/platform_version_release_dates', '', fields)

            Logger.log(f"Fetching platform versions...")
            platform_versions = self._fetch_map('/platform_versions')
            for x in platform_versions:
                if 'companies' in platform_versions[x]:
                    tx = []
                    for y in platform_versions[x]['companies']:
                        cinf = self.companies.get_row(y)
                        if cinf:
                            tx.append({ 'id': y, 'name': cinf['name'] })
                    platform_versions[x]['companies'] = tx
                if 'main_manufacturer' in platform_versions[x]:
                    platform_versions[x].pop('main_manufacturer')
                if 'platform_logo' in platform_versions[x]:
                    imginf = platform_logos[platform_versions[x]['platform_logo']]
                    if imginf:
                        imgpath = self.img_dir + '/platform_' + str(platform_versions[x]['slug']) + '.jpg'
                        platform_versions[x]['platform_logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                        if os.path.exists(imgpath):
                            platform_versions[x]['platform_logo'] = imgpath
                        else:
                            platform_versions[x]['platform_logo'] = download_file(imgpath, platform_versions[x]['platform_logo_url'])
                if 'platform_version_release_dates' in platform_versions[x]:
                    platform_versions[x]['release_dates'] = []
                    for y in platform_versions[x]['platform_version_release_dates']:
                        if y in platform_version_release_dates:
                            rdinf = { 'date': platform_version_release_dates[y]['human'], 'region': self.support_data['release_regions'][platform_version_release_dates[y]['release_region']] }
                            platform_versions[x]['release_dates'].append(rdinf)
                    platform_versions[x].pop('platform_version_release_dates')

            Logger.log(f"Fetching platforms...")
            self.platforms.reset()
            fields = ['id', 'name', 'abbreviation', 'alternative_name', 'generation', 'slug']
            resp = self._req('/platforms', 'fields *; limit 500; sort name asc;')
            for x in resp:
                y = DataTable.extract_fields(x, fields)
                if 'platform_type' in x:
                    y['platform_type'] = self.support_data['platform_types'][x['platform_type']]
                if 'platform_logo' in x:
                    imginf = platform_logos[x['platform_logo']]
                    imgpath = self.img_dir + '/platform_' + str(x['slug']) + '.jpg'
                    y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                    if os.path.exists(imgpath):
                        y['logo'] = imgpath
                    else:
                        y['logo'] = download_file(imgpath, y['logo_url'])
                if 'platform_family' in x:
                    y['platform_family'] = self.support_data['platform_families'][x['platform_family']]
                if 'versions' in x and len(x['versions']) > 0:
                    y['versions'] = []
                    for ver in x['versions']:
                        y['versions'].append(platform_versions[ver])
                self.platforms.add_row(y)

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.platforms.save()
            Logger.log(f"IGDB platform data loaded")

    def load_franchises(self):
        """ Load game franchises """
        Logger.sysmsg(f"Loading franchises data from IGDB...")
        if self.franchises.has_file():
            # Lood local data
            self.franchises.load()
            Logger.log(f"IGDB franchises data loaded from {self.franchises.filepath} - {self.franchises.count()} entries")
        else:
            # Import data
            def proc_row(y):
                y['count'] = len(y['games']) if 'games' in y else 0
                self.franchises.add_row(y)

            Logger.log(f"Fetching franchises...")
            self.franchises.reset()
            self._fetch_table('/franchises', sort='name', fp=proc_row)

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.franchises.save()
            Logger.log(f"IGDB franchises data loaded")

    def load_collections(self):
        """ Load game collections """
        Logger.sysmsg(f"Loading collections data from IGDB...")
        if self.collections.has_file():
            # Lood local data
            self.collections.load()
            Logger.log(f"IGDB collections data loaded from {self.collections.filepath} - {self.collections.count()} entries")
        else:
            # Import data
            def proc_row(y):
                y['count'] = len(y['games']) if 'games' in y else 0
                self.collections.add_row(y)

            Logger.log(f"Fetching collections...")
            self.collections.reset()
            self._fetch_table('/collections', sort='name', fp=proc_row)

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.collections.save()
            Logger.log(f"IGDB collections data loaded")

    def load_game_engines(self):
        """ Load game engines """
        Logger.sysmsg(f"Loading game engine data from IGDB...")
        if self.engines.has_file():
            # Lood local data
            self.engines.load()
            Logger.log(f"IGDB game engine data loaded from {self.engines.filepath} - {self.engines.count()} entries")
        else:
            # Import data
            Logger.log(f"Fetching game engine logos...")
            engine_logos = DataTable('', self.schema_images)
            def proc_logos_row(y):
                y['url'] = 'https:' + y['url'].replace("/t_thumb/", "/t_original/")
                engine_logos.add_row(y)

            self._fetch_table('/game_engine_logos', fp=proc_logos_row)

            Logger.log(f"Fetching game engines...")
            def proc_engine_row(y):
                if 'platforms' in y:
                    rx = []
                    for z in y['platforms']:
                        pinf = self.platforms.get_row(z)
                        if not pinf:
                            continue
                        rx.append({ 'id': pinf['id'], 'name': pinf['name'] })
                    y['platforms'] = rx
                if 'logo' in y:
                    imginf = engine_logos.get_row(y['logo'])
                    imgpath = self.img_dir + '/engine_' + str(y['slug']) + '.jpg'
                    if os.path.exists(imgpath):
                        y['logo'] = imgpath
                    else:
                        y['logo'] = download_file(imgpath, imginf['url'])
                    y['logo_url'] = imginf['url']
                if 'companies' in y:
                    rx = []
                    for z in y['companies']:
                        cinf = self.companies.get_row(z)
                        if not cinf:
                            continue
                        rx.append({ 'id': cinf['id'], 'name': cinf['name'] })
                    y['companies'] = rx
                self.engines.add_row(y)

            self._fetch_table('/game_engines', fp=proc_engine_row)

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.engines.save()
            Logger.log(f"IGDB game engine data loaded")

    def load_game_manifest(self):
        """ Load games manifest """
        Logger.sysmsg(f"Loading game data from IGDB...")
        if self.games_manifest.has_file():
            # Lood local data
            self.games_manifest.load()
            Logger.log(f"IGDB game dictionary data loaded from {self.games_manifest.filepath} - {self.games_manifest.count()} entries")
        else:
            # Import data
            Logger.log(f"Fetching alternative names...")
            cache_alt_names = DataTable()
            cache_alt_names.data = self._fetch_table('/alternative_names', fields='id, name')
            cache_alt_names.index_rows()
            Logger.log(f"Fetching release dates...")
            cache_release_dates = DataTable()
            cache_release_dates.data = self._fetch_table('/release_dates', fields='id, human, release_region, platform')
            cache_release_dates.index_rows()
            Logger.log(f"Fetching involved companies...")
            cache_involved_companies = DataTable()
            cache_involved_companies.data = self._fetch_table('/involved_companies', fields='id, company, developer, publisher, porting, supporting')
            cache_involved_companies.index_rows()

            Logger.log(f"Fetching game dictionary...")
            self.games_manifest.reset()
            self.games_manifest.data = self._fetch_table('/games', fields='id, name, slug, game_status, game_type, platforms, genres, release_dates, involved_companies, alternative_names')

            Logger.log(f"Resolving references in game dictionary. {self.games_manifest.count()} total games...")
            for i in range(len(self.games_manifest.data)):
                self.games_manifest.index[self.games_manifest.data[i]['id']] = i
                if 'game_status' in self.games_manifest.data[i] and self.games_manifest.data[i]['game_status'] in self.support_data['game_status']:
                    self.games_manifest.data[i]['game_status'] = self.support_data['game_status'][self.games_manifest.data[i]['game_status']]
                if 'game_type' in self.games_manifest.data[i] and self.games_manifest.data[i]['game_type'] in self.support_data['game_types']:
                    self.games_manifest.data[i]['game_type'] = self.support_data['game_types'][self.games_manifest.data[i]['game_type']]
                if 'genres' in self.games_manifest.data[i]:
                    self.games_manifest.data[i]['genres'] = self._ref_field(self.games_manifest.data[i]['genres'], self.support_data['genres'])
                if 'release_dates' in self.games_manifest.data[i]:
                    np = []
                    for xid in self.games_manifest.data[i]['release_dates']:
                        xdata = cache_release_dates.get_row(xid)
                        ydata = { 'date': xdata['human'] }
                        if 'release_region' in xdata and xdata['release_region'] in self.support_data['release_regions']:
                            ydata['region'] = self.support_data['release_regions'][xdata['release_region']]
                        if 'status' in xdata and xdata['status'] in self.support_data['release_status']:
                            ydata['status'] = self.support_data['release_status'][xdata['status']]
                        if 'platform' in xdata:
                            pinf = self.platforms.get_row(xdata['platform'])
                            if pinf:
                                ydata['platform'] = pinf['name']
                        np.append(ydata)
                    self.games_manifest.data[i]['release_dates'] = np
                if 'involved_companies' in self.games_manifest.data[i]:
                    self.games_manifest.data[i]['developers'] = []
                    self.games_manifest.data[i]['publishers'] = []
                    for xid in self.games_manifest.data[i]['involved_companies']:
                        xdata = cache_involved_companies.get_row(xid)
                        cinf = self.companies.get_row(xdata['company'])
                        if cinf:
                            xinf = { 'id': cinf['id'], 'name': cinf['name'] }
                            if 'publisher' in xdata and xdata['publisher']:
                                self.games_manifest.data[i]['publishers'].append(xinf)
                            if 'developer' in xdata and xdata['developer']:
                                if 'porting' in xdata and xdata['porting']:
                                    xinf['porting'] = True
                                if 'supporting' in xdata and xdata['supporting']:
                                    xinf['supporting'] = True
                                self.games_manifest.data[i]['developers'].append(xinf)
                            elif ('porting' in xdata and xdata['porting']) or ('supporting' in xdata and xdata['supporting']):
                                if 'porting' in xdata and xdata['porting']:
                                    xinf['porting'] = True
                                if 'supporting' in xdata and xdata['supporting']:
                                    xinf['supporting'] = True
                                self.games_manifest.data[i]['developers'].append(xinf)
                if 'alternative_names' in self.games_manifest.data[i]:
                    np = []
                    for xid in self.games_manifest.data[i]['alternative_names']:
                        aname = cache_alt_names.get_row(xid)
                        np.append(aname)
                    self.games_manifest.data[i]['alternative_names'] = np
                if 'platforms' in self.games_manifest.data[i]:
                    np = []
                    for pid in self.games_manifest.data[i]['platforms']:
                        pinf = self.platforms.get_row(pid)
                        if pinf:
                            np.append({ 'id': pid, 'name': pinf['name'] })
                    self.games_manifest.data[i]['platforms'] = np

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.games_manifest.save()
            Logger.log(f"IGDB games dictionary loaded")

    def calc_platform_stats(self):
        """ Calculate platform statistics """
        fpath = self.data_dir + '/' + self.platform_stats_file
        platforms_stats = { 0: { 'name': '** Games with no platform data **', 'total': 0, 'active': 0, 'games': 0, 'exp': 0, 'remakes': 0, 'bundles': 0 }}
        Logger.log(f"Calculating game platform stats...")
        count = 0
        for game in self.games_manifest.data:
            if not game['platforms'] or len(game['platforms']) == 0:
                platforms_stats[0]['total'] += 1
                if 'game_status' not in game or game['game_status'] == '' or game['game_status'] == 'Released':
                    platforms_stats[0]['active'] += 1
                if 'game_type' in game:
                    if game['game_type'] == 'Main Game':
                        platforms_stats[0]['games'] += 1
                    elif game['game_type'] == 'Remaster' or game['game_type'] == 'Remake':
                        platforms_stats[0]['remakes'] += 1
                    elif game['game_type'] == 'Bundle' or game['game_type'] == 'Expanded Game':
                        platforms_stats[0]['bundles'] += 1
                    else:
                        platforms_stats[0]['exp'] += 1
            else:
                for pinf in game['platforms']:
                    if pinf['id'] in platforms_stats:
                        platforms_stats[pinf['id']]['total'] += 1
                    else:
                        platforms_stats[pinf['id']] = { 'name': pinf['name'], 'total': 1, 'active': 0, 'games': 0, 'exp': 0, 'remakes': 0, 'bundles': 0 }
                    if 'game_status' not in game or game['game_status'] == '' or game['game_status'] == 'Released':
                        platforms_stats[pinf['id']]['active'] += 1
                    if 'game_type' in game:
                        if game['game_type'] == 'Main Game':
                            platforms_stats[pinf['id']]['games'] += 1
                        elif game['game_type'] == 'Remaster' or game['game_type'] == 'Remake':
                            platforms_stats[pinf['id']]['remakes'] += 1
                        elif game['game_type'] == 'Bundle' or game['game_type'] == 'Expanded Game':
                            platforms_stats[pinf['id']]['bundles'] += 1
                        else:
                            platforms_stats[pinf['id']]['exp'] += 1
            count += 1
            if count % 1000 == 0:
                Logger.report_progress(f"Processing games", count, self.games_manifest.count())

        # Write platform statistics
        Logger.log("Number of games per platform")
        Logger.log("  id  |                         name                          |  total | active |  games | remak | bundle |  exp  ")
        Logger.log("===================================================================================================================")
        for stat in sorted(platforms_stats.items(), key=lambda x: x[1]['total'], reverse=True):
            Logger.log(f" {stat[0]:4}  {stat[1]['name']:55}: {stat[1]['total']:6} | {stat[1]['active']:6} | {stat[1]['games']:6} | {stat[1]['remakes']:5} | {stat[1]['bundles']:5} | {stat[1]['exp']:5}")

        # Store statistics in a file
        Logger.log(f"Writing platform stats to a file...")
        DataTable.save_table(dict(sorted(platforms_stats.items())), fpath, ['id', 'name', 'total', 'active', 'games', 'remakes', 'bundles', 'exp'])

    def import_games(self):
        """ Import games using the IGDB sources configuration """
        cpath = 'config/igdbsources.json'
        if not os.path.exists(cpath):
            return
        sources = json.load(open(cpath))
        Logger.sysmsg(f"Importing games from the IGDB...")
        if self.games_manifest.count() == 0:
            Logger.warning(f"Aborting operation. Games manifest not loaded...")
            return

        # Index platform games
        reindex = False
        skipunsorted = 'skipunsorted' in sources and sources['skipunsorted']
        for pinf in self.platforms.data:
            fpath = self.gameindex_dir + f'/gameindex_igdb_{pinf['slug']}.csv'
            if not os.path.exists(fpath):
                reindex = True
                break
        if not reindex and not skipunsorted and not os.path.exists(self.gameindex_dir + f'/gameindex_igdb_unsorted.csv'):
            reindex = True
        if reindex:
            self._index_platform_games(skipunsorted)
        else:
            # Load platform indices
            for pinf in self.platforms.data:
                self._load_platform_games(pinf['id'], pinf['name'], pinf['slug'])
            if not skipunsorted:
                self._load_platform_games(0, "Unsorted games", "unsorted")

        # Validating platform sources
        Logger.log(f"Validating IGDB sources...")
        for pid in sources['platforms']:
            pinf = self.platforms.get_row(pid)
            if not pinf:
                Logger.warning(f"Skipping platform {pid}. Invalid platform ID...")
                continue

        # Import game cards
        # TODO: Add game status to platform index, remove platform name from release dates

    def import_game_screenshots(self, gid: int, loadimg: bool = True) -> list:
        """ Load game screenshots """
        ret = []
        xdata = self._req('/screenshots', f'fields *; limit 500; where game = {gid};')
        cnt = 1
        for ximg in xdata:
            ssurl = "https:" + ximg['url'].replace("/t_thumb/", "/t_original/")
            sspath = f"{self.screenshot_dir}/screenshot_{gid}_{cnt}.jpg"
            if loadimg and not os.path.exists(sspath):
                sspath = download_file(sspath, ssurl)
            if sspath:
                imginf = { 'path': sspath, 'url': ssurl }
                ret.append(imginf)
            cnt += 1
        return ret

    def import_game_artwork(self, gid: int, loadimg: bool = True) -> list:
        """ Load game artwork """
        ret = []
        if 'artwork_types' not in self.support_data or len(self.support_data['artwork_types']) == 0:
            Logger.warning("Unable to load game artwork. Support data not loaded")
            return ret
        xdata = self._req('/artworks', f'fields *; limit 500; where game = {gid};')
        cnt = 1
        for ximg in xdata:
            atype = self.support_data['artwork_types'][ximg['artwork_type']] if 'artwork_type' in ximg and ximg['artwork_type'] in self.support_data['artwork_types'] else None
            awurl = "https:" + ximg['url'].replace("/t_thumb/", "/t_original/")
            awpath = f"{self.artwork_dir}/artwork_{gid}_{cnt}.jpg"
            if loadimg and not os.path.exists(awpath):
                awpath = download_file(awpath, awurl)
            if awpath:
                imginf = { 'path': awpath, 'url': awurl, 'type': atype }
                ret.append(imginf)
            cnt += 1
        return ret

    def import_game(self, gid: int, loadscreenshots: bool = True, loadartwork: bool = True, overwrite: bool = False):
        """ Import and store game card """
        Logger.log(f"Importing game card for game ID: {gid}")
        if gid not in self.games_manifest_index:
            Logger.warning(f"Skipping - Invalid game ID: {gid}")
            return
        gname = self.games_manifest[self.games_manifest_index[gid]]['name']
        gslug = self.games_manifest[self.games_manifest_index[gid]]['slug']
        Logger.set_context(f"{gid}: {gname}")
        Logger.dbgmsg(f"Game found")
        fpath = self.gamecards_dir + f'/{gid:06}_{gslug}.json'
        if not overwrite and os.path.exists(fpath):
            Logger.dbgmsg(f"Game card already downloaded")
            Logger.clear_context()
            return

        Logger.dbgmsg(f"Fetching game info...")
        resp = self._req('/games', f'fields *; where id = {gid};')
        if not resp or len(resp) == 0:
            Logger.warning(f"No game available on IGDB")
            Logger.clear_context()
            return
        Logger.dbgmsg(f"Resolving references...")
        game_inf = self._parse_game(resp[0], True, loadscreenshots, loadartwork)

        Logger.dbgmsg(f"Saving game card...")
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(game_inf, f,  indent=4)
        Logger.log(f"Game card for saved to {fpath}")
        Logger.clear_context()

    def update_game_ratings(self):
        """ Update game ratings """
        if self.games_manifest.count() == 0:
            return

        Logger.log(f"Fetching game ratings...")
        game_ratings = DataTable()
        game_ratings.data = self._fetch_table('/games', fields='id, aggregated_rating, rating')

        Logger.log(f"Updating games manifest...")
        cnt = 0
        for rx in game_ratings.data:
            gid = rx['id']
            if 'aggregated_rating' in rx:
                self.games_manifest.get_row(gid)['metascore'] = int(round(rx['aggregated_rating']))
            if 'rating' in rx:
                self.games_manifest.get_row(gid)['rating'] = int(round(rx['rating']))
            if 'rating' in rx or 'aggregated_rating' in rx:
                cnt += 1

        Logger.log(f"Writing data to a file...")
        self.games_manifest.save()
        Logger.log(f"Ratings updated for {cnt} games...")

    def list_games(self) -> list:
        """ List all games """
        total_games = self.count_games()
        count = 0
        ret = []
        while count < total_games:
            resp = self._req('/games/', f'fields *; offset {count}; limit 500; sort name asc;')
            for x in range(0, len(resp)):
                Logger.dbgmsg(f'Game {x:3}: {resp[x]['name']}')
                game_inf = self._parse_game(resp[x])
                ret.append(game_inf)
        return ret

    def count_games(self) -> int:
        """ Count all games """
        resp = self._req('/games/count', f'')
        if resp and 'count' in resp:
            Logger.dbgmsg(f'Total game count: {resp['count']}')
            return resp['count']
        return 0

    def _count(self, url: str, query: str = '') -> int:
        """ Count data records """
        resp = self._req(url if url.endswith('/count') else url + '/count', query)
        return resp['count'] if resp and 'count' in resp else 0

    def _req(self, url: str, data: str) -> dict|None:
        """ Execute a REST API request """
        # Check last request timestamp in order to adhere to the rate limits
        self._check_limits()

        # Send a request
        response = requests.post(self.hostname_api + url, data, headers={ 'Client-ID': self.clientid, 'Authorization': 'Bearer ' + self.accesstoken })
        self.lastreqtime = time.time_ns()
        if response is None:
            return None
        return response.json()

    def _fetch(self, url: str, query: str, params: list) -> list:
        """ Fetch data from the IGDB and extract data fields"""
        ret = []
        resp = self._req(url, query)
        for x in resp:
            z = DataTable.extract_fields(x, params)
            ret.append(z)
        return ret

    def _fetch_map(self, url: str, query: str = '', params: list = None) -> dict:
        """ Fetch data from the IGDB and extract data fields"""
        total = self._count(url, query)
        Logger.log(f"{total} entries found. Importing data...")
        count = 0
        ret = {}
        while count < total:
            resp = self._req(url, f'fields *; offset {count}; limit 500; sort id asc; {query};')
            for x in resp:
                if not params or len(params) == 0:
                    ret[x['id']] = x
                elif len(params) == 2:
                    if params[0] in x and params[1] in x:
                        ret[x[params[0]]] = x[params[1]]
                else:
                    z = DataTable.extract_fields(x, params)
                    ret[x[params[0]]] = z
            count += len(resp)
            if total > 1000:
                Logger.dbgmsg(f"{count} / {total} entries loaded...")
        return ret

    def _fetch_table(self, url: str, query: str = '', cols: list = None, fields: str = '*', sort: str = 'id', fp = None) -> list:
        """ Fetch data table """
        total = self._count(url, query)
        Logger.log(f"{total} entries found. Importing data...")
        count = 0
        ret = []
        while count < total:
            resp = self._req(url, f'fields {fields}; offset {count}; limit 500; sort {sort} asc; {query};')
            for x in resp:
                y = DataTable.extract_fields(x, cols) if cols else x
                if fp:
                    fp(y)
                else:
                    ret.append(y)
            count += len(resp)
            if total > 1000:
                Logger.report_progress("Loading entries", count, total)
        return ret

    def _check_limits(self):
        """ Check request limits """
        if self.lastreqtime > 0 and time.time_ns() - self.lastreqtime < self.reqlimitms * 1000000:
            dursec = (self.reqlimitms * 1000000 - (time.time_ns() - self.lastreqtime)) / 1000000000.0
            time.sleep(dursec)

    def _extract_date(self, dt, dformat: str) -> str:
        """ Extract and serialize timestamp """
        if dformat == "YYYY":
            return dt.strftime("%Y")
        if dformat == "YYYYMM":
            return dt.strftime("%Y-%m")
        if dformat == "YYYYQ1":
            return dt.strftime("%Y") + "-02"
        if dformat == "YYYYQ2":
            return dt.strftime("%Y") + "-05"
        if dformat == "YYYYQ3":
            return dt.strftime("%Y") + "-08"
        if dformat == "YYYYQ4":
            return dt.strftime("%Y") + "-11"
        return dt.strftime("%Y-%m-%d")

    def _ref_field(self, src: list, data: list|dict, cols: list = None) -> list:
        """ Resolve field references """
        ret = []
        remap = cols and len(cols) > 0
        if type(data) is list:
            # Resolve references from a data table
            for x in src:
                xinf = next((item for item in data if item["id"] == x), None)
                if not xinf:
                    continue
                if remap and type(data[x]) is dict:
                    ret.append(DataTable.extract_fields(xinf, cols))
                else:
                    ret.append(xinf)
        else:
            # Resolve references from a data map
            for x in src:
                if x in data:
                    if remap and type(data[x]) is dict:
                        ret.append(DataTable.extract_fields(data[x], cols))
                    else:
                        ret.append(data[x])
        return ret

    def _ref_entry(self, rid: int, dt: DataTable, cols: list = None) -> dict|None:
        """ Resolve row reference """
        if dt.in_index(rid):
            xinf = dt.get_row(rid)
            ret = DataTable.extract_fields(xinf, cols)
            ret['id'] = rid
            return ret
        return None

    def _ref_game(self, src: list|int) -> list|dict|None:
        """ Resolve game reference(s) """
        if type(src) is list:
            ret = []
            for xid in src:
                if not self.games_manifest.in_index(xid):
                    Logger.warning(f"Error resolving game reference {xid}")
                    continue
                ginf = { 'id': xid, 'name': self.games_manifest.get_row(xid)['name'] }
                ret.append(ginf)
            return ret
        elif type(src) is int:
            if not self.games_manifest.in_index(src):
                Logger.warning(f"Error resolving game reference {src}")
                return None
            return { 'id': src, 'name': self.games_manifest.get_row(src)['name'] }
        else:
            return None

    def _index_platform_games(self, skipunsorted: bool):
        """ Index games for all platform """
        # Importing data
        self.games_plaforms_index = {}
        Logger.log(f"Creating platform indices...")
        for gamerow in self.games_manifest.data:
            gid = gamerow['id']
            if 'platforms' in gamerow and gamerow['platforms'] and len(gamerow['platforms']) > 0:
                for pinf in gamerow['platforms']:
                    if pinf['id'] not in self.games_plaforms_index:
                        self.games_plaforms_index[pinf['id']] = []
                    self.games_plaforms_index[pinf['id']].append(gid)
            elif not skipunsorted:
                if 0 not in self.games_plaforms_index:
                    self.games_plaforms_index[0] = []
                self.games_plaforms_index[0].append(gid)
            else:
                Logger.dbgmsg(f"No platform data for {gamerow['name']} (ID: {gid})")

        # Save data
        for pid in self.games_plaforms_index:
            count = 0
            total = len(self.games_plaforms_index[pid])
            game_data = []
            pname = "Unsorted games" if pid == 0 else self.platforms.get_row(pid)['name']
            pslug = "unsorted" if pid == 0 else self.platforms.get_row(pid)['slug']
            Logger.log(f"Composing index data for platform '{pname}'...")
            for gid in self.games_plaforms_index[pid]:
                rdata = DataTable.extract_fields(self.games_manifest.get_row(gid), self.schema_platform_index)
                if 'release_dates' in rdata and rdata['release_dates']:
                    rx = []
                    relyear = 0
                    for reldinf in rdata['release_dates']:
                        if 'platform' in reldinf and reldinf['platform'] and reldinf['platform'] != pname:
                            continue
                        rdyear = extract_year(reldinf['date'])
                        if relyear == 0 or rdyear < relyear:
                            relyear = rdyear
                        if 'platform' in reldinf:
                            reldinf.pop('platform')
                        rx.append(reldinf)
                    rdata['release_dates'] = rx
                    rdata['year'] = relyear if relyear > 0 else None

                game_data.append(rdata)
                count += 1
                if total > 5000 and (count == 1 or count % 2000 == 0 or count == total):
                    Logger.report_progress(f"Formatting data for platform '{pname}'", count, total)

            Logger.dbgmsg(f"Saving index for platform '{pname}'...")
            fpath = self.gameindex_dir + f'/gameindex_igdb_{pslug}.csv'
            DataTable.save_table(game_data, fpath, self.schema_platform_index)

    def _load_platform_games(self, pid: int, pname: str, pslug: str):
        """ Load platform index """
        Logger.dbgmsg(f"Loading platform index for {pname}...")
        fpath = self.gameindex_dir + f'/gameindex_igdb_{pslug}.csv'
        if not os.path.exists(fpath):
            Logger.error(f"Missing index for platform {pname}")
            return
        self.games_plaforms_index[pid] = []
        table = DataTable.load_table(fpath, ['id'])
        for row in table:
            self.games_plaforms_index[pid].append(row['id'])

    def _parse_game(self, data: dict, loadplatforms: bool = False, loadscreenshots: bool = True, loadartwork: bool = True) -> dict:
        """ Parse game data """
        params = ['id', 'name', 'summary', 'storyline' 'aggregated_rating', 'aggregated_rating_count', 'rating', 'rating_count', 'total_rating', 'total_rating_count', 'hypes', 'version_title', 'url', 'slug']
        grefs = ['parent_game', 'similar_games', 'bundles', 'dlcs', 'expanded_games', 'expansions', 'forks', 'ports', 'remakes', 'remasters', 'standalone_expansions', 'version_parent']
        ret = DataTable.extract_fields(data, params)
        if not self.support_data or len(self.support_data) == 0:
            return ret
        for col in grefs:
            if col in data:
                ret[col] = self._ref_game(data[col])
        if 'first_release_date' in data and data['first_release_date'] > 0:
            ret['first_release_date'] = datetime.datetime.fromtimestamp(data['first_release_date']).strftime("%Y-%m-%d")
        if 'game_status' in data and data['game_status'] in self.support_data['game_status']:
            ret['game_status'] = self.support_data['game_status'][data['game_status']]
        if 'game_type' in data and data['game_type'] in self.support_data['game_types']:
            ret['game_type'] = self.support_data['game_types'][data['game_type']]
        if 'game_modes' in data:
            ret['game_modes'] = self._ref_field(data['game_modes'], self.support_data['game_modes'])
        if 'genres' in data:
            ret['genres'] = self._ref_field(data['genres'], self.support_data['genres'])
        if 'release_dates' in data:
            ret['release_dates'] = []
            for xid in data['release_dates']:
                xdata = self._req('/release_dates', f'fields *; limit 500; where id = {xid};')[0]
                ydata = { 'date': xdata['human'] }
                if 'release_region' in xdata and xdata['release_region'] in self.support_data['release_regions']:
                    ydata['region'] = self.support_data['release_regions'][xdata['release_region']]
                if 'status' in xdata and xdata['status'] in self.support_data['release_status']:
                    ydata['status'] = self.support_data['release_status'][xdata['status']]
                if 'platform' in xdata:
                    pinf = self.platforms.get_row(xdata['platform'])
                    if pinf:
                        ydata['platform'] = pinf['name']
                ret['release_dates'].append(ydata)
        if 'alternative_names' in data:
            xdata = self._req('/alternative_names', f'fields *; limit 500; where game = {ret['id']};')
            ret['alternative_names'] = []
            for x in xdata:
                ret['alternative_names'].append(x['name'])
        if 'involved_companies' in data:
            ret['developers'] = []
            ret['publishers'] = []
            ret['porting'] = []
            ret['support'] = []
            for xid in data['involved_companies']:
                xdata = self._req('/involved_companies', f'fields *; limit 500; where id = {xid};')[0]
                cinf = self.companies.get_row(xdata['company'])
                if cinf:
                    xinf = { 'id': cinf['id'], 'name': cinf['name'] }
                    if 'developer' in xdata and xdata['developer']:
                        ret['developers'].append(xinf)
                    if 'publisher' in xdata and xdata['publisher']:
                        ret['publishers'].append(xinf)
                    if 'porting' in xdata and xdata['porting']:
                        ret['porting'].append(xinf)
                    if 'supporting' in xdata and xdata['supporting']:
                        ret['supporting'].append(xinf)
                    if len(ret['porting']) == 0:
                        ret.pop('porting')
                    if len(ret['support']) == 0:
                        ret.pop('support')
        if loadplatforms and 'platforms' in data:
            ret['platforms'] = []
            for pid in data['platforms']:
                pinf = self.platforms.get_row(pid)
                if not pinf:
                    continue
                ret['platforms'].append(pinf['name'])
        if 'language_supports' in data:
            ret['languages'] = []
            xdata = self._req('/language_supports', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['language_supports']):
                Logger.warning(f"Support languages missmatch for game {ret['name']}")
            for x in xdata:
                lang = self.support_data['languages'][x['language']] if x['language'] in self.support_data['languages'] else None
                ltype = self.support_data['language_support_types'][x['language_support_type']] if x['language_support_type'] in self.support_data['language_support_types'] else None
                ret['languages'].append({ 'lang': lang, 'type': ltype })
        if 'player_perspectives' in data:
            ret['player_perspectives'] = self._ref_field(data['player_perspectives'], self.support_data['player_perspectives'])
        if 'keywords' in data:
            ret['keywords'] = self._ref_field(data['keywords'], self.support_data['keywords'])
        if 'themes' in data:
            ret['themes'] = self._ref_field(data['themes'], self.support_data['themes'])
        if 'game_localizations' in data:
            ret['localizations'] = []
            xdata = self._req('/game_localizations', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['game_localizations']):
                Logger.warning(f"Game localizations missmatch for game {ret['name']}")
            for x in xdata:
                reg = self.support_data['regions'][x['region']]['name'] if 'region' in x and x['region'] in self.support_data['regions'] else None
                ret['localizations'].append({ 'name': x['name'], 'region': reg })
        if 'age_ratings' in data:
            ret['age_ratings'] = []
            for x in data['age_ratings']:
                xdata = self._req('/age_ratings', f'fields *; limit 500; where id = {x};')[0]
                if not xdata:
                    continue
                arinf = {
                    'organization': self.support_data['age_rating_organizations'][xdata['organization']],
                    'rating': self.support_data['age_rating_categories'][xdata['rating_category']]['rating']
                }
                if 'rating_content_descriptions' in xdata:
                    arinf['description'] = []
                    for y in xdata['rating_content_descriptions']:
                        arinf['description'].append(self.support_data['age_rating_descriptions'][y]['description'])
                ret['age_ratings'].append(arinf)
        if 'multiplayer_modes' in data:
            xdata = self._req('/multiplayer_modes', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['multiplayer_modes']):
                Logger.warning(f"Invalid multiplayer modes for game '{ret['name']}'")
            modes = ['campaigncoop', 'dropin', 'lancoop', 'offlinecoop', 'onlinecoop', 'splitscreen', 'splitscreenonline']
            pcounts = ['offlinecoopmax', 'offlinemax', 'onlinecoopmax', 'onlinemax']
            ret['multiplayer_modes'] = []
            for x in xdata:
                pinf = self.platforms.get_row(x['platform']) if 'platform' in x else None
                pname = pinf['name'] if pinf else ''
                mp_platf_inf = { 'platform': pname, 'multiplayer_modes': [] }
                for y in modes:
                    if y in x and x[y]:
                        mp_platf_inf['multiplayer_modes'].append(y)
                for y in pcounts:
                    if y in x:
                        mp_platf_inf[y] = x[y]
                ret['multiplayer_modes'].append(mp_platf_inf)
        if 'game_engines' in data:
            ret['game_engines'] = []
            for x in data['game_engines']:
                xinf = self.engines.get_row(x)
                if xinf:
                    ret['game_engines'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'collections' in data:
            ret['collections'] = []
            for x in data['collections']:
                xinf = self.collections.get_row(x)
                if xinf:
                    ret['collections'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'franchise' in data:
            ret['franchises'] = []
            xinf = self.franchises.get_row(data['franchise'])
            if xinf:
                ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'franchises' in data:
            ret['franchises'] = []
            for x in data['franchises']:
                xinf = self.franchises.get_row(x)
                if xinf:
                    ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'cover' in data:
            xdata = self._req('/covers', f'fields *; limit 500; where id = {data['cover']};')[0]
            covurl = "https:" + xdata['url'].replace("/t_thumb/", "/t_original/")
            covpath = f"{self.covers_dir}/cover_{data['slug']}_{ret['id']}.jpg"
            if not os.path.exists(covpath):
                covpath = download_file(covpath, covurl)
            if covpath:
                ret['cover'] = { 'path': covpath, 'url': covurl }
        if 'screenshots' in data:
            ret['screenshots'] = self.import_game_screenshots(ret['id'], loadscreenshots)
        if 'artworks' in data:
            ret['artworks'] = self.import_game_artwork(ret['id'], loadartwork)
        if 'videos' in data:
            ret['videos'] = []
            xdata = self._req('/game_videos', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['videos']):
                Logger.warning(f"Invalid videos for game '{ret['name']}'")
            for x in xdata:
                ret['videos'].append({ 'name': x['name'], 'url': 'https://www.youtube.com/watch?v=' + x['video_id'] })
        if 'websites' in data:
            ret['websites'] = []
            xdata = self._req('/websites', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['websites']):
                Logger.warning(f"Invalid web sites for game '{ret['name']}'")
            for x in xdata:
                ret['websites'].append(x['url'])
        if 'external_games' in data:
            ret['external_sources'] = []
            xdata = self._req('/external_games', f'fields *; limit 500; where game = {ret['id']};')
            for x in xdata:
                exinf = DataTable.extract_fields(x, ['name', 'url', 'year'])
                if 'countries' in x:
                    exinf['countries'] = []
                    for cid in x['countries']:
                        if str(cid) in self.countries:
                            exinf['countries'].append(self.countries[str(cid)])
                        else:
                            Logger.warning(f"Invalid country reference {cid} for external game source")
                if 'game_release_format' in x and x['game_release_format'] in self.support_data['game_release_formats']:
                    exinf['format'] = self.support_data['game_release_formats'][x['game_release_format']]
                if 'external_game_source' in x and x['external_game_source'] in self.support_data['external_game_sources']:
                    exinf['source'] = self.support_data['external_game_sources'][x['external_game_source']]
                if 'platform' in x:
                    pinf = self.platforms.get_row(x['platform'])
                    if pinf:
                        exinf['platform'] = pinf['name']
                    else:
                        Logger.warning(f"Invalid platform reference {x['platform']} for external game source")
                ret['external_sources'].append(exinf)

        # Check time to beat stats
        xdata = self._req('/game_time_to_beats', f'fields *; limit 500; where game_id = {ret['id']};')
        if xdata and len(xdata) > 0:
            xdata = xdata[0]
            if 'count' in xdata and xdata['count'] > 0:
                ret['time_count'] = xdata['count']
            if 'normally' in xdata and xdata['normally'] > 0:
                ret['time_normal'] = seconds_to_hours(xdata['normally'])
            if 'hastily' in xdata and xdata['hastily'] > 0:
                ret['time_minimal'] = seconds_to_hours(xdata['hastily'])
            if 'completely' in xdata and xdata['completely'] > 0:
                ret['time_full'] = seconds_to_hours(xdata['completely'])
        return ret
