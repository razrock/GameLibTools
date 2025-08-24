"""
    Game Library Tools
    IGDB REST API client

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import csv
import json
import os
import time
from io import BytesIO
from datetime import datetime

import requests
from gamelibtools.logger import Logger


class IgdbClient:
    """ IGDB REST API client """

    def __init__(self):
        """ Class constructor """
        self.clientid = ''
        self.clientsecret = ''
        self.data_dir = 'data'
        self.img_dir = 'data/images'
        self.screenshot_dir = 'data/screenshots'
        self.covers_dir = 'data/covers'
        self.artwork_dir = 'data/artwork'
        self.support_data_file = 'igdb_support.json'
        self.support_data = {}
        self.platforms_file = 'igdb_platforms.csv'
        self.platforms = []
        self.companies_file = 'igdb_companies.csv'
        self.companies = []
        self.franchises_file = 'igdb_franchises.csv'
        self.franchises = []
        self.collections_file = 'igdb_collections.csv'
        self.collections = []
        self.engines_file = 'igdb_engines.csv'
        self.engines = []
        self.hostname_auth = 'https://id.twitch.tv/oauth2/token'
        self.hostname_api = 'https://api.igdb.com/v4'
        self.hostname_img = 'https://images.igdb.com/image/upload'
        self.accesstoken = ''
        self.reqlimitms = 250
        self.lastreqtime = 0
        self.countries = {}
        self.schema_platforms = ['id', 'name', 'abbreviation', 'alternative_name', 'platform_type', 'platform_family', 'logo', 'logo_url', {'name': 'generation', 'type': 'int'}, 'slug', {'name': 'versions', 'type': 'list'}]
        self.schema_companies = ['id', 'name', 'description', 'start_date', 'logo', 'logo_url', 'country', 'status', 'parent', 'changed_company', 'change_date', 'slug', 'developed', 'published']
        self.schema_images = ['id', 'name', 'image_id', 'url', {'name': 'width', 'type': 'int'}, {'name': 'height', 'type': 'int'}]
        self.schema_franchises = ['id', 'name', 'url', { 'name': 'count', 'type': 'int' }, {'name': 'games', 'type': 'list'}]
        self.schema_collections = ['id', 'name', 'url', { 'name': 'count', 'type': 'int' }, {'name': 'games', 'type': 'list'}]
        self.schema_engines = ['id', 'name', 'slug', 'description', 'url', 'logo', 'logo_url', {'name': 'companies', 'type': 'list'}, {'name': 'platforms', 'type': 'list'}]
        self.schema_games = ['id', 'name', 'summary', 'storyline' 'aggregated_rating', 'aggregated_rating_count', 'rating', 'rating_count', 'total_rating', 'total_rating_count',
                             'hypes', 'parent_game', 'similar_games', 'bundles', 'dlcs', 'expanded_games', 'expansions', 'forks', 'ports', 'remakes', 'remasters',
                             'standalone_expansions', 'version_title', 'version_parent', 'first_release_date', 'game_status', 'game_type', 'game_modes', 'genres',
                             'release_dates', 'alternative_names', 'developers', 'publishers', 'languages', 'player_perspectives', 'tags', 'keywords', 'themes', 'localizations',
                             'age_ratings', 'multiplayer_modes', 'offlinecoopmax', 'offlinemax', 'onlinecoopmax', 'onlinemax', 'game_engines', 'collections', 'franchises',
                             'cover', 'screenshots', 'artworks', 'videos', 'websites', 'external_sources']
        # TODO: Fix games schema + Add features
        # - Popularity
        # - Time to beat
        # - Platform versions
        # - Game versions
        # - Characters


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

        self.countries = json.load(open('config/countries.json'))
        self.load_common_data()
        self.load_platforms()
        self.load_companies()
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
            self.support_data['platform_types'] = self._fetch_map('/platform_types', 'fields *; limit 500;', fields)
            for pid in self.support_data['platform_types']:
                self.support_data['platform_types'][pid] = self.support_data['platform_types'][pid].replace("_", " ")

            Logger.log(f"Fetching platform families...")
            fields = ['id', 'name']
            self.support_data['platform_families'] = self._fetch_map('/platform_families', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching game types...")
            fields = ['id', 'type']
            self.support_data['game_types'] = self._fetch_map('/game_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching game statuses...")
            fields = ['id', 'status']
            self.support_data['game_status'] = self._fetch_map('/game_statuses', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching game modes...")
            fields = ['id', 'name']
            self.support_data['game_modes'] = self._fetch_map('/game_modes', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching regions...")
            fields = ['id', 'name', 'identifier', 'category']
            self.support_data['regions'] = self._fetch_map('/regions', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching player perspectives...")
            fields = ['id', 'name']
            self.support_data['player_perspectives'] = self._fetch_map('/player_perspectives', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching genres...")
            fields = ['id', 'name']
            self.support_data['genres'] = self._fetch_map('/genres', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching themes...")
            fields = ['id', 'name']
            self.support_data['themes'] = self._fetch_map('/themes', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching keywords...")
            fields = ['id', 'name']
            self.support_data['keywords'] = {}
            total_keywords = self._count('/keywords/count', '')
            count = 0
            while count < total_keywords:
                resp = self._fetch_map('/keywords', f'fields *; offset {count}; limit 500;', fields)
                self.support_data['keywords'].update(resp)
                count += len(resp)

            Logger.log(f"Fetching release date regions...")
            fields = ['id', 'region']
            self.support_data['release_regions'] = self._fetch_map('/release_date_regions', 'fields *; limit 500;', fields)
            for x in self.support_data['release_regions']:
                self.support_data['release_regions'][x] = self.support_data['release_regions'][x].replace("_", " ").capitalize()

            Logger.log(f"Fetching release date statuses...")
            fields = ['id', 'name']
            self.support_data['release_status'] = self._fetch_map('/release_date_statuses', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching date formats...")
            fields = ['id', 'format']
            self.support_data['date_formats'] = self._fetch_map('/date_formats', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching company statuses...")
            fields = ['id', 'name']
            self.support_data['company_status'] = self._fetch_map('/company_statuses', 'fields *; limit 500;', fields)
            for x in self.support_data['company_status']:
                self.support_data['company_status'][x] = self.support_data['company_status'][x].capitalize()

            Logger.log(f"Fetching language support types...")
            fields = ['id', 'name']
            self.support_data['language_support_types'] = self._fetch_map('/language_support_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching languages...")
            fields = ['id', 'name', 'native_name', 'locale']
            self.support_data['languages'] = self._fetch_map('/languages', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching collection relation types...")
            fields = ['id', 'name']
            self.support_data['collection_relation_types'] = self._fetch_map('/collection_relation_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching collection membership types...")
            fields = ['id', 'name']
            self.support_data['collection_membership_types'] = self._fetch_map('/collection_membership_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching artwork types...")
            fields = ['id', 'name']
            self.support_data['artwork_types'] = self._fetch_map('/artwork_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching game release formats...")
            fields = ['id', 'format']
            self.support_data['game_release_formats'] = self._fetch_map('/game_release_formats', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching external game sources...")
            fields = ['id', 'name']
            self.support_data['external_game_sources'] = self._fetch_map('/external_game_sources', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching age rating organizations...")
            fields = ['id', 'name']
            self.support_data['age_rating_organizations'] = self._fetch_map('/age_rating_organizations', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching age rating categories...")
            fields = ['id', 'rating', 'organization']
            self.support_data['age_rating_categories'] = self._fetch_map('/age_rating_categories', 'fields *; limit 500;', fields)
            for x in self.support_data['age_rating_categories']:
                oid = self.support_data['age_rating_categories'][x]['organization']
                self.support_data['age_rating_categories'][x]['organization'] = self.support_data['age_rating_organizations'][oid]

            Logger.log(f"Fetching age rating content description types...")
            fields = ['id', 'name']
            self.support_data['age_rating_description_types'] = self._fetch_map('/age_rating_content_description_types', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching age rating content descriptions...")
            fields = ['id', 'description', 'description_type', 'organization']
            self.support_data['age_rating_descriptions'] = self._fetch_map('/age_rating_content_descriptions_v2', 'fields *; limit 500;', fields)
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

    def load_platforms(self):
        """ Load platform data """
        Logger.sysmsg(f"Loading platform data from IGDB...")
        fpath = self.data_dir + '/' + self.platforms_file
        if os.path.exists(fpath):
            # Lood local platforms data
            self.platforms = self.load_data_table(fpath, self.schema_platforms)

            # Check platform logos
            for platform in self.platforms:
                if 'logo' in platform and platform['logo']:
                    if not os.path.exists(platform['logo']):
                        Logger.warning(f"Platform {platform['name']} logo missing...")
                        self._download(platform['logo'], platform['logo_url'])
            Logger.log(f"IGDB platform data loaded from {fpath}")
        else:
            # Import data
            Logger.log(f"Fetching platform versions...")
            fields = ['id', 'name']
            platform_versions = self._fetch_map('/platform_versions', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching platform logos...")
            fields = ['id', 'name', 'image_id', 'url', 'width', 'height', 'animated', 'alpha_channel']
            platform_logos = self._fetch_map('/platform_logos', 'fields *; limit 500;', fields)

            Logger.log(f"Fetching platforms...")
            self.platforms = []
            fields = ['id', 'name', 'abbreviation', 'alternative_name', 'generation', 'slug']
            resp = self._req('/platforms', 'fields *; limit 500; sort name asc;')
            for x in resp:
                y = self._extract_fields(x, fields)
                if 'platform_type' in x:
                    y['platform_type'] = self.support_data['platform_types'][x['platform_type']]
                if 'platform_logo' in x:
                    imginf = platform_logos[x['platform_logo']]
                    imgpath = self.img_dir + '/platform_' + str(x['slug']) + '.jpg'
                    y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                    if os.path.exists(imgpath):
                        y['logo'] = imgpath
                    else:
                        y['logo'] = self._download(imgpath, y['logo_url'])
                if 'platform_family' in x:
                    y['platform_family'] = self.support_data['platform_families'][x['platform_family']]
                if 'versions' in x and len(x['versions']) > 1:
                    y['versions'] = []
                    for ver in x['versions']:
                        y['versions'].append(platform_versions[ver])
                self.platforms.append(y)

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.platforms, fpath, self.schema_platforms)
            Logger.log(f"IGDB platform data loaded")

    def load_companies(self):
        """ Load companies data """
        Logger.sysmsg(f"Loading company data from IGDB...")
        fpath = self.data_dir + '/' + self.companies_file
        if os.path.exists(fpath):
            # Lood local platforms data
            self.companies = self.load_data_table(fpath, self.schema_companies)

            # Check logos
            for company in self.companies:
                if 'logo' in company and company['logo']:
                    if not os.path.exists(company['logo']):
                        Logger.warning(f"Company {company['name']} logo missing...")
                        self._download(company['logo'], company['logo_url'])
                company['developed'] = int(company['developed'])
                company['published'] = int(company['published'])
            Logger.log(f"IGDB company data loaded from {fpath}")
        else:
            # Import data
            cachepath = self.data_dir + '/igdb_cache_company_logos.csv'
            company_logos = []
            company_logos_map = {}
            if os.path.exists(cachepath):
                company_logos = self.load_data_table(cachepath, self.schema_images)
                for i in range(len(company_logos)):
                    company_logos_map[int(company_logos[i]['id'])] = i
            else:
                Logger.log(f"Fetching company logos...")
                total_company_logos = self._count('/company_logos/count', '')
                count = 0
                while count < total_company_logos:
                    resp = self._req('/company_logos', f'fields *; offset {count}; limit 500;')
                    for x in resp:
                        y = self._extract_fields(x, self.schema_images)
                        company_logos.append(y)
                        company_logos_map[y['id']] = len(company_logos) - 1
                    count += len(resp)
                self.save_data_table(company_logos, cachepath, self.schema_images)

            Logger.log(f"Fetching companies...")
            self.companies = []
            company_map = {}
            total_companies = self._count('/companies/count', '')
            count = 0
            fields = ['id', 'name', 'description', 'slug', 'changed_company_id', 'parent']
            while count < total_companies:
                resp = self._req('/companies', f'fields *; offset {count}; limit 500; sort name asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    if 'status' in x:
                        if x['status'] in self.support_data['company_status']:
                            y['status'] = self.support_data['company_status'][x['status']]
                        else:
                            Logger.warning(f"Invalid company status: {x['status']}")
                    if 'logo' in x:
                        if x['logo'] in company_logos_map and company_logos_map[x['logo']] < len(company_logos):
                            imginf = company_logos[company_logos_map[x['logo']]]
                            imgpath = self.img_dir + '/' + 'company_' + str(x['slug']) + '.jpg'
                            y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                            if os.path.exists(imgpath):
                                y['logo'] = imgpath
                            else:
                                y['logo'] = self._download(imgpath, y['logo_url'])
                        else:
                            Logger.warning(f"Company '{x['name']}' logo reference invalid: {x['logo']}")
                    if 'start_date' in x and x['start_date'] > 0:
                        try:
                            dt_object = datetime.fromtimestamp(x['start_date'])
                            if 'start_date_format' in x:
                                y['start_date'] = self._extract_date(dt_object, self.support_data['date_formats'][x['start_date_format']])
                            else:
                                y['start_date'] = dt_object.strftime("%Y-%m-%d")
                        except Exception as e:
                            Logger.warning(f"Company '{x['name']}' established date parsing failed. {e}")
                    if 'change_date' in x and x['change_date'] > 0:
                        try:
                            dt_object = datetime.fromtimestamp(x['change_date'])
                            if 'change_date_format' in x:
                                y['change_date'] = self._extract_date(dt_object, self.support_data['date_formats'][x['change_date_format']])
                            else:
                                y['change_date'] = dt_object.strftime("%Y-%m-%d")
                        except Exception as e:
                            Logger.warning(f"Company '{x['name']}' changed date parsing failed. {e}")
                    if 'country' in x:
                        if str(x['country']) in self.countries:
                            y['country'] = self.countries[str(x['country'])]
                        else:
                            Logger.warning(f"Unknown country code: {x['country']}")
                    y['developed'] = len(x['developed']) if 'developed' in x else 0
                    y['published'] = len(x['published']) if 'published' in x else 0
                    self.companies.append(y)
                    company_map[y['id']] = len(self.companies) - 1
                count += len(resp)
                Logger.dbgmsg(f"{count} / {total_companies} companies loaded...")

            # Connect companies
            for cinf in self.companies:
                if 'parent' in cinf:
                    pid = cinf['parent']
                    if pid in company_map and company_map[pid] < len(self.companies):
                        xinf = self.companies[company_map[pid]]
                        cinf['parent'] = { 'id': pid, 'name': xinf['name'] }
                    else:
                        Logger.warning(f"Company '{cinf['name']}' parent reference invalid: {pid}")
                if 'changed_company_id' in cinf:
                    pid = cinf['changed_company_id']
                    if pid in company_map and company_map[pid] < len(self.companies):
                        xinf = self.companies[company_map[pid]]
                        cinf['changed_company'] = { 'id': pid, 'name': xinf['name'] }
                        cinf.pop('changed_company_id')
                    else:
                        Logger.warning(f"Company '{cinf['name']}' changed company reference invalid: {pid}")

                        # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.companies, fpath, self.schema_companies)
            Logger.log(f"IGDB company data loaded")

            # Clear cache
            os.remove(cachepath)

    def load_franchises(self):
        """ Load game franchises """
        Logger.sysmsg(f"Loading franchises data from IGDB...")
        fpath = self.data_dir + '/' + self.franchises_file
        if os.path.exists(fpath):
            # Lood local data
            self.franchises = self.load_data_table(fpath, self.schema_franchises)
            Logger.log(f"IGDB franchises data loaded from {fpath}")
        else:
            # Import data
            Logger.log(f"Fetching franchises...")
            total = self._count('/franchises/count', '')
            Logger.log(f"{total} franchises found. Importing data...")
            count = 0
            fields = ['id', 'name', 'url', 'games']
            self.franchises = []
            while count < total:
                resp = self._req('/franchises', f'fields *; offset {count}; limit 500; sort name asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    y['count'] = len(x['games']) if 'games' in x else 0
                    self.franchises.append(y)
                count += len(resp)
                Logger.dbgmsg(f"{count} / {total} franchises loaded...")

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.franchises, fpath, self.schema_franchises)
            Logger.log(f"IGDB franchises data loaded")

    def load_collections(self):
        """ Load game collections """
        Logger.sysmsg(f"Loading collections data from IGDB...")
        fpath = self.data_dir + '/' + self.collections_file
        if os.path.exists(fpath):
            # Lood local data
            self.collections = self.load_data_table(fpath, self.schema_collections)
            Logger.log(f"IGDB collections data loaded from {fpath}")
        else:
            # Import data
            Logger.log(f"Fetching collections...")
            total = self._count('/collections/count', '')
            Logger.log(f"{total} collections found. Importing data...")
            count = 0
            fields = ['id', 'name', 'url', 'games']
            self.collections = []
            while count < total:
                resp = self._req('/collections', f'fields *; offset {count}; limit 500; sort name asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    y['count'] = len(x['games']) if 'games' in x else 0
                    self.collections.append(y)
                count += len(resp)
                Logger.dbgmsg(f"{count} / {total} collections loaded...")

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.collections, fpath, self.schema_collections)
            Logger.log(f"IGDB collections data loaded")

    def load_game_engines(self):
        """ Load game engines """
        Logger.sysmsg(f"Loading game engine data from IGDB...")
        fpath = self.data_dir + '/' + self.engines_file
        if os.path.exists(fpath):
            # Lood local data
            self.engines = self.load_data_table(fpath, self.schema_engines)
            Logger.log(f"IGDB game engine data loaded from {fpath}")
        else:
            # Import data
            Logger.log(f"Fetching game engine logos...")
            total = self._count('/game_engine_logos/count', '')
            Logger.log(f"{total} game engine logos found. Importing data...")
            count = 0
            engine_logos = {}
            fields = ['id', 'image_id', 'url', 'width', 'height']
            while count < total:
                resp = self._req('/game_engine_logos', f'fields *; offset {count}; limit 500; sort id asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    y['url'] = 'https:' + y['url'].replace("/t_thumb/", "/t_original/")
                    engine_logos[y['id']] = y
                count += len(resp)

            Logger.log(f"Fetching game engines...")
            total = self._count('/game_engines/count', '')
            Logger.log(f"{total} game engines found. Importing data...")
            fields = ['id', 'name', 'slug', 'description', 'url']
            self.engines = []
            count = 0
            while count < total:
                resp = self._req('/game_engines', f'fields *; offset {count}; limit 500; sort name asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    if 'platforms' in x:
                        y['platforms'] = []
                        for z in x['platforms']:
                            pinf = next((item for item in self.platforms if item["id"] == z), None)
                            if not pinf:
                                continue
                            y['platforms'].append({ 'id': pinf['id'], 'name': pinf['name'] })
                    if 'logo' in x:
                        imginf = engine_logos[x['logo']]
                        imgpath = self.img_dir + '/engine_' + str(x['slug']) + '.jpg'
                        if os.path.exists(imgpath):
                            y['logo'] = imgpath
                        else:
                            y['logo'] = self._download(imgpath, imginf['url'])
                        y['logo_url'] = imginf['url']
                    if 'companies' in x:
                        y['companies'] = []
                        for z in x['companies']:
                            cinf = next((item for item in self.companies if item["id"] == z), None)
                            if not cinf:
                                continue
                            y['companies'].append({ 'id': cinf['id'], 'name': cinf['name'] })
                    self.engines.append(y)
                count += len(resp)
                Logger.dbgmsg(f"{count} / {total} game engines loaded...")

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.engines, fpath, self.schema_engines)
            Logger.log(f"IGDB game engine data loaded")

    def import_games(self):
        """ Import games using the IGDB sources configuration """
        cpath = 'config/igdbsource.json'
        if not os.path.exists(cpath):
            return
        Logger.sysmsg(f"Importing game from the IGDB...")
        sources = json.load(open(cpath))
        for platform_name in sources:
            data_path = self.data_dir + '/gamedata_igdb_' + platform_name + '.csv'
            if os.path.exists(data_path):
                Logger.log(f"Data table for platform '{platform_name}' found. Skipping...")
                continue
            if 'platforms' not in sources[platform_name] or len(sources[platform_name]['platforms']) == 0:
                Logger.log(f"No data sources defined for platform '{platform_name}'. Skipping...")
                continue

            # Importing data
            game_data = []
            for pid in sources[platform_name]['platforms']:
                if len(game_data) == 0:
                    game_data = self.list_platform_games(pid)
                else:
                    game_data.extend(self.list_platform_games(pid))

            # Saving data
            if len(game_data) > 0:
                Logger.log(f"Saving data for platform '{platform_name}'...")
                self.save_data_table(game_data, data_path, self.schema_games)

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

    def import_platform_games(self, pid: int):
        """ Import and store platform games """
        pinf = next((item for item in self.platforms if item["id"] == pid), None)
        if not pinf:
            return
        games = self.list_platform_games(pid)
        if len(games) > 0:
            Logger.log(f"Saving data...")
            fpath = f"{self.data_dir}/gamedata_igdb_{pinf['slug']}.csv"
            self.save_data_table(games, fpath, self.schema_games)

    def list_platform_games(self, pid: int) -> list:
        """ List platform games """
        pinf = next((item for item in self.platforms if item["id"] == pid), None)
        if not pinf:
            return []
        Logger.sysmsg(f'Listing games for platform {pinf['name']}...')
        total_games = self._count('/games/count', f'where platforms = {pid};')
        Logger.log(f"{total_games} games found. Importing data...")
        count = 0
        ret = []
        while count < total_games:
            resp = self._req('/games/', f'fields *; offset {count}; limit 500; sort name asc; where platforms = {pid};')
            for x in range(0, len(resp)):
                Logger.dbgmsg(f'{x:5}: {resp[x]['name']}')
                game_inf = self._parse_game(resp[x])
                ret.append(game_inf)
            count += len(resp)
        return ret

    def count_platform_games(self, pid: int) -> int:
        """ Count platform games """
        resp = self._req('/games/count', f'where platforms = {pid};')
        if resp and 'count' in resp:
            Logger.dbgmsg(f'Platform game count: {resp['count']}')
            return resp['count']
        return 0

    def import_game_screenshots(self, gid: int) -> list:
        """ Load game screenshots """
        ret = []
        xdata = self._req('/screenshots', f'fields *; limit 500; where game = {gid};')
        cnt = 1
        for ximg in xdata:
            ssurl = "https:" + ximg['url'].replace("/t_thumb/", "/t_original/")
            sspath = f"{self.screenshot_dir}/screenshot_{gid}_{cnt}.jpg"
            if not os.path.exists(sspath):
                sspath = self._download(sspath, ssurl)
            if sspath:
                imginf = { 'path': sspath, 'url': ssurl }
                ret.append(imginf)
            cnt += 1
        return ret

    def import_game_artwork(self, gid: int) -> list:
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
            if not os.path.exists(awpath):
                awpath = self._download(awpath, awurl)
            if awpath:
                imginf = { 'path': awpath, 'url': awurl, 'type': atype }
                ret.append(imginf)
            cnt += 1
        return ret

    def save_data_table(self, rows: list, fpath: str, schema: list):
        """
        Export games data table to a CSV file
        :param rows: Data table
        :param fpath: File path
        :param schema: Data table schema
        """
        with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
            cols = self._get_schema_columns(schema)
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            for row in rows:
                rdata = self._list_fields(row, schema)
                writer.writerow(rdata)
        Logger.log(f"Data table stored to {fpath}")

    def load_data_table(self, fpath: str, schema: list) -> list:
        """ Load data table """
        ret = []
        checkheader = False
        rownum = 0
        cols = self._get_schema_columns(schema)
        with open(fpath, 'r', newline='\r\n', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            try:
                for row in reader:
                    if not checkheader:
                        if len(row) != len(schema):
                            Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch")
                            return []
                        for i in range(len(row)):
                            if row[i] != cols[i]:
                                Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch (column {i})")
                                return []
                        checkheader = True
                        continue
                    y = self._parse_fields(row, schema)
                    ret.append(y)
                    rownum += 1
            except Exception as e:
                Logger.error(f"Parsing data table {fpath} failed, row {rownum}. {e}")
        return ret

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
            z = self._extract_fields(x, params)
            ret.append(z)
        return ret

    def _fetch_map(self, url: str, query: str, params: list) -> dict:
        """ Fetch data from the IGDB and extract data fields"""
        ret = {}
        if len(params) < 2:
            return ret
        resp = self._req(url, query)
        for x in resp:
            if len(params) == 2:
                if params[0] in x and params[1] in x:
                    ret[x[params[0]]] = x[params[1]]
            else:
                z = self._extract_fields(x, params)
                ret[x[params[0]]] = z
        return ret

    def _get_image(self, fname: str, imghash: str, imgsize: str, query: str) -> str|None:
        """ Download image from the IGDB """
        # Check last request timestamp in order to adhere to the rate limits
        self._check_limits()

        # Send a request
        url = '/t_' + imgsize + '/' + imghash + '.jpg'
        response = requests.post(self.hostname_img + url, query, headers={ 'Client-ID': self.clientid, 'Authorization': 'Bearer ' + self.accesstoken })
        self.lastreqtime = time.time_ns()
        if response is None:
            return None

        imgbytes = BytesIO(response.content)
        fpath = self.img_dir + '/' + fname
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)
        with open(fpath, 'wb') as f:
            f.write(imgbytes.getbuffer())
        return fpath

    def _download(self, fpath: str, url: str) -> str|None:
        """ Download image from the IGDB """
        Logger.log(f"Downloading image {fpath} from {url}...")
        try:
            response = requests.get(url)
            if response is None:
                return None

            imgbytes = BytesIO(response.content)
            with open(fpath, 'wb') as f:
                f.write(imgbytes.getbuffer())
            return fpath
        except Exception as e:
            Logger.error(f"Downloading image {fpath} from {url} failed. {e}")
            return None

    def _check_limits(self):
        """ Check request limits """
        if self.lastreqtime > 0 and time.time_ns() - self.lastreqtime < self.reqlimitms * 1000000:
            dursec = (self.reqlimitms * 1000000 - (time.time_ns() - self.lastreqtime)) / 1000000000.0
            time.sleep(dursec)

    def _extract_fields(self, src: dict, params: list) -> dict:
        """ Extract fields from a object (dict) """
        z = {}
        for y in params:
            if y in src:
                z[y] = src[y]
        return z

    def _list_fields(self, src: dict, schema: list) -> list:
        """ List data fields in a data row """
        z = []
        for y in schema:
            name = y if type(y) is str else y['name']
            dtyp = ('int' if name == 'id' else 'str') if type(y) is str else y['type']
            if name in src and src[name]:
                if dtyp == 'list' or dtyp == 'dict':
                    z.append(json.dumps(src[name]))
                elif dtyp == 'int':
                    z.append(int(src[name]))
                elif dtyp == 'float':
                    z.append(float(src[name]))
                else:
                    z.append(src[name])
            else:
                z.append(None)
        return z

    def _parse_fields(self, src: list, schema: list) -> dict:
        """ Parse data row """
        ret = {}
        for i in range(len(schema)):
            if i >= len(src):
                break
            try:
                name = schema[i] if type(schema[i]) is str else schema[i]['name']
                dtyp = ('int' if name == 'id' else 'str') if type(schema[i]) is str else schema[i]['type']
                if dtyp == 'list' or dtyp == 'dict':
                    ret[name] = json.loads(src[i]) if src[i] != "" else None
                elif dtyp == 'int':
                    ret[name] = int(src[i]) if src[i] != "" else None
                elif dtyp == 'float':
                    ret[name] = float(src[i]) if src[i] != "" else None
                else:
                    ret[name] = src[i]
            except Exception as e:
                raise Exception(f"Parsing data column {i}: {schema[i]} failed - {src[i]}")
        return ret

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

    def _get_schema_columns(self, schema: list) -> list:
        """ Parse schema columns """
        ret = []
        for c in schema:
            if type(c) is str:
                ret.append(c)
            elif type(c) is dict and 'name' in c:
                ret.append(c['name'])
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return ret

    def _parse_game(self, data: dict, loadplatforms: bool = False, loadscreenshots: bool = True, loadartwork: bool = True) -> dict:
        """ Parse game data """
        params = ['id', 'name', 'summary', 'storyline' 'aggregated_rating', 'aggregated_rating_count', 'rating', 'rating_count', 'total_rating', 'total_rating_count',
                  'hypes', 'parent_game', 'similar_games', 'bundles', 'dlcs', 'expanded_games', 'expansions', 'forks', 'ports', 'remakes', 'remasters',
                  'standalone_expansions', 'version_title', 'version_parent']
        ret = self._extract_fields(data, params)
        if 'first_release_date' in data and data['first_release_date'] > 0:
            ret['first_release_date'] = datetime.fromtimestamp(data['first_release_date']).strftime("%Y-%m-%d")
        if 'game_status' in data and data['game_status'] in self.support_data['game_status']:
            ret['game_status'] = self.support_data['game_status'][data['game_status']]
        if 'game_type' in data and data['game_type'] in self.support_data['game_types']:
            ret['game_type'] = self.support_data['game_types'][data['game_type']]
        if 'game_modes' in data:
            ret['game_modes'] = []
            for x in data['game_modes']:
                if x in self.support_data['game_modes']:
                    ret['game_modes'].append(self.support_data['game_modes'][x])
        if 'genres' in data:
            ret['genres'] = []
            for x in data['genres']:
                if x in self.support_data['genres']:
                    ret['genres'].append(self.support_data['genres'][x])
        if 'release_dates' in data:
            ret['release_dates'] = []
            for xid in data['release_dates']:
                xdata = self._req('/release_dates', f'fields *; limit 500; where id = {xid};')[0]
                ydata = { 'date': xdata['human'] }
                if 'release_region' in xdata and xdata['release_region'] in self.support_data['release_regions']:
                    ydata['region'] = self.support_data['release_regions'][xdata['release_region']]
                if 'status' in xdata and xdata['status'] in self.support_data['release_status']:
                    ydata['status'] = self.support_data['release_status'][xdata['status']]
                ret['release_dates'].append(ydata)
        if 'alternative_names' in data:
            xdata = self._req('/alternative_names', f'fields *; limit 500; where game = {ret['id']};')
            ret['alternative_names'] = []
            for x in xdata:
                ret['alternative_names'].append(x['name'])
        if 'involved_companies' in data:
            ret['developers'] = []
            ret['publishers'] = []
            for xid in data['involved_companies']:
                xdata = self._req('/involved_companies', f'fields *; limit 500; where id = {xid};')[0]
                cinf = next((item for item in self.companies if item["id"] == xdata['company']), None)
                if cinf:
                    xinf = { 'id': cinf['id'], 'name': cinf['name'] }
                    if 'developer' in xdata and xdata['developer']:
                        ret['developers'].append(xinf)
                    if 'publisher' in xdata and xdata['publisher']:
                        ret['publishers'].append(xinf)
        if loadplatforms and 'platforms' in data:
            ret['platforms'] = []
            for pid in data['platforms']:
                pinf = next((item for item in self.platforms if item["id"] == pid), None)
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
            ret['player_perspectives'] = []
            for x in data['player_perspectives']:
                if x in self.support_data['player_perspectives']:
                    ret['player_perspectives'].append(self.support_data['player_perspectives'][x])
        if 'keywords' in data:
            ret['keywords'] = []
            for x in data['keywords']:
                if x in self.support_data['keywords']:
                    ret['keywords'].append(self.support_data['keywords'][x])
        if 'tags' in data:
            ret['tags'] = []
            for x in data['tags']:
                tag_type = (x & 0xf0000000) >> 28
                tag_id = x & 0x0fffffff
                tag_source = None
                match tag_type:
                    case 0:
                        # Resolve Theme
                        tag_source = self.support_data['themes']
                    case 1:
                        # Resolve genre
                        tag_source = self.support_data['genres']
                    case 2:
                        # Resolve keyword
                        tag_source = self.support_data['keywords']
                    case 3:
                        # Resolve games
                        Logger.warning(f"Tag represents a game reference, ID: {tag_id}")
                    case 4:
                        # Resolve player perspective
                        tag_source = self.support_data['player_perspectives']
                if tag_source and tag_id in tag_source:
                    ret['tags'].append(tag_source[tag_id])
        if 'themes' in data:
            ret['themes'] = []
            for x in data['themes']:
                if x in self.support_data['themes']:
                    ret['themes'].append(self.support_data['themes'][x])
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
            if len(xdata) > 1:
                Logger.warning(f"Multiple multiplayer modes for game '{ret['name']}'")
            ret['multiplayer_modes'] = []
            modes = ['campaigncoop', 'dropin', 'lancoop', 'offlinecoop', 'onlinecoop', 'splitscreen', 'splitscreenonline']
            pcounts = ['offlinecoopmax', 'offlinemax', 'onlinecoopmax', 'onlinemax']
            for x in xdata:
                for y in modes:
                    if y in x and x[y]:
                        ret['multiplayer_modes'].append(y)
                for y in pcounts:
                    if y in x:
                        ret[y] = x[y]
        if 'game_engines' in data:
            ret['game_engines'] = []
            for x in data['game_engines']:
                xinf = next((item for item in self.engines if item["id"] == x), None)
                if xinf:
                    ret['game_engines'].append({ 'id': xinf['id'], 'name': xinf['name'] })
        if 'collections' in data:
            ret['collections'] = []
            for x in data['collections']:
                xinf = next((item for item in self.collections if item["id"] == x), None)
                if xinf:
                    ret['collections'].append({ 'id': xinf['id'], 'name': xinf['name'] })
        if 'franchise' in data:
            ret['franchises'] = []
            xinf = next((item for item in self.franchises if item["id"] == data['franchise']), None)
            if xinf:
                ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'] })
        if 'franchises' in data:
            ret['franchises'] = []
            for x in data['franchises']:
                xinf = next((item for item in self.franchises if item["id"] == x), None)
                if xinf:
                    ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'] })
        if 'cover' in data:
            xdata = self._req('/covers', f'fields *; limit 500; where id = {data['cover']};')[0]
            covurl = "https:" + xdata['url'].replace("/t_thumb/", "/t_original/")
            covpath = f"{self.covers_dir}/cover_{data['slug']}_{ret['id']}.jpg"
            if not os.path.exists(covpath):
                covpath = self._download(covpath, covurl)
            if covpath:
                ret['cover'] = { 'path': covpath, 'url': covurl }
        if loadscreenshots and 'screenshots' in data:
            ret['screenshots'] = self.import_game_screenshots(ret['id'])
        if loadartwork and 'artworks' in data:
            ret['artworks'] = self.import_game_artwork(ret['id'])
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
                exinf = self._extract_fields(x, ['name', 'url', 'year'])
                if 'countries' in x:
                    exinf['countries'] = []
                    for cid in x['countries']:
                        if cid in self.countries:
                            exinf['countries'].append(self.countries[cid])
                        else:
                            Logger.warning(f"Invalid country reference {cid} for external game source")
                if 'game_release_format' in x and x['game_release_format'] in self.support_data['game_release_formats']:
                    exinf['format'] = self.support_data['game_release_formats'][x['game_release_format']]
                if 'external_game_source' in x and x['external_game_source'] in self.support_data['external_game_sources']:
                    exinf['source'] = self.support_data['external_game_sources'][x['external_game_source']]
                if 'platform' in x:
                    pinf = next((item for item in self.platforms if item["id"] == x['platform']), None)
                    if pinf:
                        exinf['platform'] = pinf['name']
                    else:
                        Logger.warning(f"Invalid platform reference {x['platform']} for external game source")
                ret['external_sources'].append(exinf)
        return ret
