"""
    Game Library Tools
    Wikipedia table importer

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import os
import requests
import string
from gamelibtools.platformdataset import *
from gamelibtools.platforminfo import *
from bs4 import BeautifulSoup


""" Wikipedia per platform data source mappings """
wiki_import_map = {
    'xbox': {
        'url': 'https://en.wikipedia.org/wiki/List_of_Xbox_games',
        'schema': ["Title", "Developers", "Publishers", "Released PAL", "Released JP", "Released NA"]
    },
    'ps1': {
        'url': ['https://en.wikipedia.org/wiki/List_of_PlayStation_(console)_games_(A-L)', 'https://en.wikipedia.org/wiki/List_of_PlayStation_(console)_games_(M-Z)'],
        'schema': ["Title", "Developers", "Publishers", "Released JP", "Released PAL", "Released NA"]
    },
    'ps2': {
        'url': ['https://en.wikipedia.org/wiki/List_of_PlayStation_2_games_(A-K)', 'https://en.wikipedia.org/wiki/List_of_PlayStation_2_games_(L-Z)'],
        'schema': ["Title", "Developers", "Publishers", "Released", "JP", "PAL", "NA"],
        'headerrows': 1
    }
}

class GameImporter:
    """ Game importer class """

    def __init__(self):
        """ Class constructor """
        self.data_dir = "data"
        self.skip_existing = True
        self.tableid = "softwarelist"
        self.header_rows = 2
        self.letters = list(string.ascii_uppercase)
        self.letters.append('Numerical')

    def run(self, selplatform: str=''):
        """ Import all data sources (defined platforms) """
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        for platform, config in wiki_import_map.items():
            if self.skip_existing and os.path.exists(self._get_file_path(platform)) and selplatform == '':
                print(f"\nGame data for platform {platform} found, skipping platform")
                continue
            if selplatform != '' and selplatform != platform:
                continue

            platformdata = PlatformDataset()
            acttableid = config['tableid'] if 'tableid' in config else self.tableid
            actheaderrows = config['headerrows'] if 'headerrows' in config else self.header_rows
            print(f"\nImporting game data for platform {platform}")
            if 'sections' in config and config['sections']:
                for letter in self.letters:
                    acturl = config['url'].replace('{TOKEN}', letter)
                    self.import_from_wiki(platform, acturl, acttableid, config['schema'], actheaderrows, platformdata)
            elif isinstance(config['url'], list):
                for xurl in config['url']:
                    self.import_from_wiki(platform, xurl, acttableid, config['schema'], actheaderrows, platformdata)
            else:
                self.import_from_wiki(platform, config['url'], acttableid, config['schema'], actheaderrows, platformdata)

            print(f"Data extracted - {len(platformdata.games)} entries found. Exporting data...")
            if len(platformdata.games) > 0:
                platformdata.export(self._get_file_path(platform), platform, config['schema'])
            print(" ")
            platformdata.report()

    def import_from_wiki(self, platform: str, url: str, tableid: str, schema: list, headrows: int, data):
        """
        Import games table from the wikipedia page
        :param platform: Platform name
        :param url: Page URL
        :param tableid: Table element ID
        :param schema: Table schema (columns list)
        :param headrows: Number of header rows
        :param data: Game dataset [ref]
        """
        ind = 0
        for row in self._get_table_data(url, tableid):
            # Extract cell values
            if ind < headrows:
                ind += 1
                continue

            try:
                entry = []
                cind = 0
                for cell in row.contents:
                    if cell.name != "th" and cell.name != "td":
                        continue
                    entry.append(extract_html_content(cell, self._is_array_field(schema[cind])))
                    cind += 1
                    if cind >= len(schema):
                        break

                # Process data
                gameinf = PlatformInfo()
                gameinf.load(entry, schema)
                data.add(gameinf)
            except Exception as err:
                print(err)
            ind += 1

    def _get_file_path(self, platform: str):
        """ Get export file path """
        return f"{self.data_dir}/gamedata_wiki_{platform}.csv"

    def _get_table_data(self, url: str, tableid: str):
        """
        Process HTML table with game data from the wikipedia page
        :param url: Page URL
        :param tableid: Data table element ID
        """
        print(f"Fetching game data from {url}...")
        response = requests.get(url)
        html_content = response.text
        if html_content is None:
            return []

        print(f"HTML page downloaded. Parsing HTML for #{tableid} table...")
        soup = BeautifulSoup(html_content, features="html.parser")
        table = soup.find(id=tableid)
        if table is None:
            return []

        print(f"Data table found. Extracting data...")
        rows = table.find("tbody").find_all("tr")
        return rows

    def _is_array_field(self, col: str) -> bool:
        """ Check if selected column can hold multiple entries """
        if col.lower() == "title":
            return False
        return True
