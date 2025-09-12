"""
    Game Library Tools
    IGDB REST API client

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import json
import time
from gamelibtools.util import *


class IgdbClient:
    """ IGDB REST API client """
    def __init__(self):
        self.hostname_auth = 'https://id.twitch.tv/oauth2/token'
        self.hostname_api = 'https://api.igdb.com/v4'
        self.hostname_img = 'https://images.igdb.com/image/upload'
        self.clientid = ''
        self.clientsecret = ''
        self.accesstoken = ''
        self.reqlimitms = 250
        self.lastreqtime = 0
        self._init()

    def req(self, url: str, data: str) -> dict|None:
        """ Execute a REST API request """
        # Check client authentication
        if not self.is_authenticated():
            self._auth()

        # Check last request timestamp in order to adhere to the rate limits
        self._check_limits()

        # Send a request
        Logger.dbgmsg(f"Sending a IGDB request to {url} -> {data}...")
        response = requests.post(self.hostname_api + url, data, headers={ 'Client-ID': self.clientid, 'Authorization': 'Bearer ' + self.accesstoken })
        self.lastreqtime = time.time_ns()
        if response is None:
            return None
        return response.json()

    def maxval(self, url: str, col: str):
        """ Get max column value """
        resp = self.req(url, f'fields {col}; limit 1; sort {col} desc;')
        return resp[0][col] if resp and len(resp) > 0 and col in resp[0] else None

    def count(self, url: str, query: str = '') -> int:
        """ Count data records """
        resp = self.req(url if url.endswith('/count') else url + '/count', query)
        return resp['count'] if resp and 'count' in resp else 0

    def is_authenticated(self) -> bool:
        """ Check if client is autrhenticated """
        return len(self.accesstoken) > 0

    def _init(self):
        """ Load authentication data """
        authobj = json.load(open('config/igdbauth.json'))
        if not authobj:
            return
        if 'clientid' in authobj:
            self.clientid = authobj['clientid']
        if 'clientsecret' in authobj:
            self.clientsecret = authobj['clientsecret']
        Logger.dbgmsg(f"IGDB API client configuration loaded successfully, client ID: {self.clientid}")

    def _auth(self):
        """ Authenticate with the remote server """
        response = requests.post(self.hostname_auth, { 'client_id': self.clientid, 'client_secret': self.clientsecret, 'grant_type': 'client_credentials' })
        if response is None:
            return
        respobj = response.json()
        if respobj is None or 'access_token' not in respobj:
            return
        Logger.log("IGDB API client authenticated successfully")
        self.accesstoken = respobj['access_token']

    def _check_limits(self):
        """ Check request limits """
        if self.lastreqtime > 0 and time.time_ns() - self.lastreqtime < self.reqlimitms * 1000000:
            dursec = (self.reqlimitms * 1000000 - (time.time_ns() - self.lastreqtime)) / 1000000000.0
            time.sleep(dursec)
