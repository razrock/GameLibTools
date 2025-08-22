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
from gamelibtools.util import print_array


class IgdbClient:
    """ IGDB REST API client """

    def __init__(self):
        """ Class constructor """
        self.clientid = ''
        self.clientsecret = ''
        self.data_dir = 'data'
        self.img_dir = 'data/images'
        self.support_data_file = 'igdb_support.json'
        self.support_data = {}
        self.platforms_file = 'igdb_platforms.csv'
        self.platforms = []
        self.companies_file = 'igdb_companies.csv'
        self.companies = []
        self.hostname_auth = 'https://id.twitch.tv/oauth2/token'
        self.hostname_api = 'https://api.igdb.com/v4'
        self.hostname_img = 'https://images.igdb.com/image/upload'
        self.accesstoken = ''
        self.reqlimitms = 250
        self.lastreqtime = 0

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
        self.load_common_data()
        self.load_platforms()
        self.load_companies()

    def load_common_data(self):
        """ Load common data"""
        Logger.sysmsg(f"Fetching support data from IGDB...")
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
            Logger.sysmsg(f"IGDB support tables loaded from {fpath}")
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

            # Write data to a json file
            Logger.log(f"Writing data to a file...")
            with open(fpath, 'w', encoding='utf-8') as f:
                json.dump(self.support_data, f,  indent=4)
            Logger.log(f"IGDB support tables exported to {fpath}")
            Logger.sysmsg(f"IGDB support tables loaded")

    def load_platforms(self):
        """ Load platform data """
        Logger.sysmsg(f"Fetching platform data from IGDB...")
        fpath = self.data_dir + '/' + self.platforms_file
        cols = ['id', 'name', 'abbreviation', 'alternative_name', 'platform_type', 'platform_family', 'logo', 'logo_url', 'generation', 'slug', 'versions']
        if os.path.exists(fpath):
            # Lood local platforms data
            self.platforms = self.load_data_table(fpath, cols)

            # Check platform logos
            for platform in self.platforms:
                if 'logo' in platform and platform['logo']:
                    if not os.path.exists(platform['logo']):
                        Logger.warning(f"Platform {platform['name']} logo missing...")
                        self._download(platform['logo'], platform['logo_url'])
            Logger.sysmsg(f"IGGDB platform data loaded from {fpath}")
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
                    y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_screenshot_med/")
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
            self.save_data_table(self.platforms, fpath, cols)
            Logger.sysmsg(f"IGDB platform data loaded")

    def load_companies(self):
        """ Load companies data """
        Logger.sysmsg(f"Fetching company data from IGDB...")
        fpath = self.data_dir + '/' + self.companies_file
        cols = ['id', 'name', 'description', 'start_date', 'logo', 'logo_url', 'country', 'status', 'parent', 'changed_company', 'change_date', 'slug']
        if os.path.exists(fpath):
            # Lood local platforms data
            self.companies = self.load_data_table(fpath, cols)

            # Check logos
            for company in self.companies:
                if 'logo' in company and company['logo']:
                    if not os.path.exists(company['logo']):
                        Logger.warning(f"Company {company['name']} logo missing...")
                        self._download(company['logo'], company['logo_url'])
            Logger.sysmsg(f"IGGDB company data loaded from {fpath}")
        else:
            # Import data
            cachepath = self.data_dir + '/igdb_cache_company_logos.csv'
            company_logos = []
            company_logos_map = {}
            imgfields = ['id', 'name', 'image_id', 'url', 'width', 'height', 'animated', 'alpha_channel']
            if os.path.exists(cachepath):
                company_logos = self.load_data_table(cachepath, imgfields)
                for i in range(len(company_logos)):
                    company_logos_map[company_logos[i]['id']] = i
            else:
                Logger.log(f"Fetching company logos...")
                total_company_logos = self._count('/company_logos/count', '')
                count = 0
                while count < total_company_logos:
                    resp = self._req('/company_logos', f'fields *; offset {count}; limit 500;')
                    for x in resp:
                        y = self._extract_fields(x, imgfields)
                        company_logos.append(y)
                        company_logos_map[y['id']] = len(company_logos) - 1
                    count += len(resp)
                self.save_data_table(company_logos, cachepath, imgfields)

            Logger.log(f"Fetching companies...")
            self.companies = []
            company_map = {}
            total_companies = self._count('/companies/count', '')
            count = 0
            fields = ['id', 'name', 'description', 'slug', 'country', 'changed_company_id', 'parent']
            while count < total_companies:
                resp = self._req('/companies', f'fields *; offset {count}; limit 500; sort name asc;')
                for x in resp:
                    y = self._extract_fields(x, fields)
                    if 'status' in x:
                        y['status'] = self.support_data['company_status'][x['status']]
                    if 'logo' in x:
                        imginf = company_logos[x['logo']]
                        imgpath = self.img_dir + '/' + 'company_' + str(x['slug']) + '.jpg'
                        y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_720p/")
                        if os.path.exists(imgpath):
                            y['logo'] = imgpath
                        else:
                            y['logo'] = self._download(imgpath, y['logo_url'])
                    if 'start_date' in x:
                        dt_object = datetime.fromtimestamp(x['start_date'])
                        if 'start_date_format' in x:
                            y['start_date'] = dt_object.strftime(self.support_data['date_formats'][x['start_date_format']])
                        else:
                            y['start_date'] = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                    if 'change_date' in x:
                        dt_object = datetime.fromtimestamp(x['change_date'])
                        if 'change_date_format' in x:
                            y['change_date'] = dt_object.strftime(self.support_data['date_formats'][x['change_date_format']])
                        else:
                            y['change_date'] = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                    self.companies.append(y)
                    company_map[y['id']] = len(self.companies) - 1
                count += len(resp)

            # Connect companies
            for cinf in self.companies:
                if 'parent' in cinf:
                    pid = cinf['parent']
                    xinf = self.companies[company_map[pid]]
                    cinf['parent'] = { 'id': pid, 'name': xinf['name'] }
                if 'changed_company_id' in cinf:
                    pid = cinf['changed_company_id']
                    xinf = self.companies[company_map[pid]]
                    cinf['changed_company'] = { 'id': pid, 'name': xinf['name'] }
                    cinf.pop('changed_company_id')

            # Write data to a CSV file
            Logger.log(f"Writing data to a file...")
            self.save_data_table(self.companies, fpath, cols)
            Logger.sysmsg(f"IGDB company data loaded")

            # Clear cache
            os.remove(cachepath)

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
        pinf = next((item for item in self.support_data['platforms'] if item["id"] == pid), None)
        if not pinf:
            return
        games = self.list_platform_games(pid)
        cols = ["id", "name", "game_status", "game_type", "release_dates", "alternative_names", "involved_companies", "aggregated_rating", "rating"]
        if len(games) > 0:
            Logger.log(f"Saving data...")
            fpath = f"{self.data_dir}/gamedata_igdb_{pinf['slug']}.csv"
            self.save_data_table(games, fpath, cols)

    def list_platform_games(self, pid: int) -> list:
        """ List platform games """
        pinf = next((item for item in self.support_data['platforms'] if item["id"] == pid), None)
        if not pinf:
            return []
        Logger.sysmsg(f'Listing games for platform {pinf['name']}...')
        total_games = self.count_platform_games(pid)
        count = 0
        ret = []
        while count < total_games:
            resp = self._req('/games/', f'fields *; offset {count}; limit 500; sort name asc; where platforms = {pid};')
            for x in range(0, len(resp)):
                Logger.dbgmsg(f'Platform {x:3}: {resp[x]['name']}')
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

    def save_data_table(self, rows: list, fpath: str, cols: list):
        """
        Export games data table to a CSV file
        :param rows: Data table
        :param fpath: File path
        :param cols: Column list
        """
        with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            for row in rows:
                rdata = self._list_fields(row, cols)
                writer.writerow(rdata)
        Logger.log(f"Data table stored to {fpath}")

    def load_data_table(self, fpath: str, cols: list) -> list:
        """ Load data table """
        ret = []
        checkheader = False
        with open(fpath, 'r', newline='\r\n', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            for row in reader:
                if not checkheader:
                    if len(row) != len(cols):
                        Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch")
                        return []
                    for i in range(len(row)):
                        if row[i] != cols[i]:
                            Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch (column {i})")
                            return []
                    checkheader = True
                    continue
                y = self._parse_fields(row, cols)
                ret.append(y)
        return ret

    def _count(self, url: str, query: str) -> int:
        """ Count data records """
        resp = self._req(url, query)
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
        # Check last request timestamp in order to adhere to the rate limits
        Logger.log(f"Downloading image {fpath} from {url}...")
        self._check_limits()

        # Send a request
        response = requests.get(url)
        self.lastreqtime = time.time_ns()
        if response is None:
            return None

        imgbytes = BytesIO(response.content)
        with open(fpath, 'wb') as f:
            f.write(imgbytes.getbuffer())
        return fpath

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

    def _list_fields(self, src: dict, params: list) -> list:
        """ List data fields in a data row """
        z = []
        for y in params:
            if y in src:
                if type(src[y]) is list or type(src[y]) is dict:
                    z.append(json.dumps(src[y]))
                else:
                    z.append(src[y])
            else:
                z.append(None)
        return z

    def _parse_fields(self, src: list, params: list) -> dict:
        """ Parse data row """
        ret = {}
        for i in range(len(params)):
            if i >= len(src):
                break
            if type(src[i]) is str and len(src[i]) > 0:
                if (src[i][0] == '[' and src[i][-1] == ']') or (src[i][0] == '{' and src[i][-1] == '}'):
                    ret[params[i]] = json.loads(src[i])
                else:
                    ret[params[i]] = src[i]
            else:
                ret[params[i]] = src[i]
        return ret

    def _parse_game(self, data: dict, loadplatforms: bool = False) -> dict:
        """ Parse game data """
        params = ['id', 'name', 'first_release_date', 'aggregated_rating', 'rating']
        ret = self._extract_fields(data, params)
        if 'game_status' in data:
            ret['game_status'] = self.support_data['game_status'][data['game_status']]
        if 'game_type' in data:
            ret['game_type'] = self.support_data['game_types'][data['game_type']]
        if 'release_dates' in data:
            ret['release_dates'] = []
            for xid in data['release_dates']:
                xdata = self._req('/release_dates', f'fields *; limit 500; where id = {xid};')
                ydata = { 'region': xdata['human'] }
                ydata['region'] = self.support_data['release_status'][xdata['release_region']]
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
                xdata = self._req('/involved_companies', f'fields *; limit 500; where id = {xid};')
                if 'developer' in xdata and xdata['developer']:
                    ret['developers'].append(self.support_data['companies'][xdata['company']])
                if 'publisher' in xdata and xdata['publisher']:
                    ret['publishers'].append(self.support_data['companies'][xdata['company']])
        if loadplatforms and 'platforms' in data:
            ret['platforms'] = []
            for pid in data['platforms']:
                pinf = next((item for item in self.support_data['platforms'] if item["id"] == pid), None)
                if not pinf:
                    continue
                ret['platforms'].append(pinf['name'])
        return ret
