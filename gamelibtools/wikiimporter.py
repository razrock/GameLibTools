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
from gamelibtools.dataset import *
from bs4 import BeautifulSoup

""" Wikipedia per platform data source mappings """
wiki_import_map = {
    'xbox': {
        'url': 'https://en.wikipedia.org/wiki/List_of_Xbox_games',
        'table': 'softwarelist',
        'schema': ["Title", "Developers", "Publishers", "Released PAL", "Released JP", "Released NA"],
        'regions': True
    }
}

class GameImporter:
    """ Game importer class """

    def __init__(self):
        """ Class constructor """
        self.data_dir = "data"
        self.skip_existing = True
        self.letters = list(string.ascii_uppercase)
        self.letters.append('Numerical')

    def run(self):
        """ Import all data sources (defined platforms) """
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        for platform, config in wiki_import_map.items():
            if self.skip_existing and os.path.exists(self._get_file_path(platform)):
                print(f"\nGame data for platform {platform} found, skipping platform")
                continue

            platformdata = GameDataset()
            print(f"\nImporting game data for platform {platform}")
            if 'sections' in config and config['sections']:
                for letter in self.letters:
                    acturl = config['url'].replace('{TOKEN}', letter)
                    self.import_from_wiki(platform, acturl, config['table'], config['schema'], platformdata)
            elif isinstance(config['url'], list):
                for xurl in config['url']:
                    self.import_from_wiki(platform, xurl, config['table'], config['schema'], platformdata)
            else:
                self.import_from_wiki(platform, config['url'], config['table'], config['schema'], platformdata)

    def import_from_wiki(self, platform: str, url: str, tableid: str, schema: list, data):
        """
        Import games table from the wikipedia page
        :param platform: Platform name
        :param url: Page URL
        :param tableid: Table element ID
        :param schema: Table schema (columns list)
        :param data: Game dataset [ref]
        """
        for row in self._get_table_data(url, tableid):
            # Extract cell values
            maincol = row.find("th")
            if maincol is None or maincol['scope'] != "row":
                continue
            entry = [extract_html_content(maincol)]
            for cell in row.find_all("td"):
                entry.append(extract_html_content(cell))

            # Process data
            gameinf = GameInfo()
            gameinf.load_platform(platform, entry, schema)
            data.add(gameinf)

        print(f"Data extracted - {len(data.games)} entries found. Exporting data...")
        data.export(self._get_file_path(platform), platform, schema)
        print(" ")
        data.report()

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
