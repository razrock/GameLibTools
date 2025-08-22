"""
    Game Library Tools
    Wikipedia table importer

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import json
import os
import requests
import string

from gamelibtools.platformdataset import *
from gamelibtools.platforminfo import *
from bs4 import BeautifulSoup


class WikiImporter:
    """ Game importer class """

    def __init__(self):
        """ Class constructor """
        self.data_dir = "data"
        self.skip_existing = True
        self.tableid = "softwarelist"
        self.header_rows = 2
        self.letters = list(string.ascii_uppercase)
        self.letters.append('Numerical')
        self.sources_map = json.load(open('config/wikisources.json'))

    def run(self, selplatform: str=''):
        """ Import all data sources (defined platforms) """
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        for platform, config in self.sources_map.items():
            if self.skip_existing and os.path.exists(self._get_file_path(platform)) and selplatform == '':
                Logger.log(f"\nGame data for platform {platform} found, skipping platform")
                continue
            if selplatform != '' and selplatform != platform:
                continue

            platformdata = PlatformDataset()
            acttableid = config['tableid'] if 'tableid' in config else self.tableid
            actheaderrows = config['headerrows'] if 'headerrows' in config else self.header_rows
            Logger.sysmsg(f"\n** Importing game data for platform {platform.upper()}")
            Logger.sysmsg(f"====================================================================================================")
            if 'sections' in config and config['sections']:
                for letter in self.letters:
                    acturl = config['url'].replace('{TOKEN}', letter)
                    self.import_from_wiki(acturl, acttableid, config['schema'], actheaderrows, platformdata)
            elif isinstance(config['url'], list):
                for xurl in config['url']:
                    self.import_from_wiki(xurl, acttableid, config['schema'], actheaderrows, platformdata)
            else:
                # Check if there are multiple tables on the same page
                if 'tables' in config and config['tables']:
                    actschema = config['schema'] if 'schema' in config else {}
                    for table_inf in config['tables']:
                        acttableid = table_inf['tableid'] if 'tableid' in table_inf else acttableid
                        actheaderrows = table_inf['headerrows'] if 'headerrows' in table_inf else actheaderrows
                        actschema = table_inf['schema'] if 'schema' in table_inf else actschema
                        self.import_from_wiki(config['url'], acttableid, actschema, actheaderrows, platformdata)
                else:
                    self.import_from_wiki(config['url'], acttableid, config['schema'], actheaderrows, platformdata)

            # Resolve flags
            if 'flags' in config and 'Flags' in config['schema']:
                Logger.log("Resolving platform flags...")
                for gameinf in platformdata.games:
                    gameinf.resolve_flags(config['flags'])

            # Save data / Print summary
            Logger.sysmsg(f"Data extracted - {len(platformdata.games)} entries found")
            platformdata.report()
            if len(platformdata.games) > 0:
                Logger.log(f"Exporting data...")
                platformdata.export(self._get_file_path(platform))
            Logger.log(" ")

    def import_from_wiki(self, url: str, tableid: str, schema: list, headrows: int, data):
        """
        Import games table from the wikipedia page
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
                    entry.append(extract_html_content(cell))
                    cind += 1
                    if cind >= len(schema):
                        break

                # Process data
                gameinf = PlatformInfo()
                gameinf.load(entry, schema)
                data.add(gameinf)
            except Exception as err:
                Logger.error(str(err))
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
        Logger.log(f"Fetching game data from {url}...")
        if len(tableid) == 0:
            return []
        response = requests.get(url)
        html_content = response.text
        if html_content is None:
            return []

        Logger.log(f"HTML page downloaded. Parsing HTML for #{tableid} table...")
        soup = BeautifulSoup(html_content, features="html.parser")
        if tableid[0] == '_':
            # No table ID -> Query n-th table in the document
            ind = int(tableid[1:])
            all_tables = soup.findAll('table')
            if not all_tables or len(all_tables) == 0:
                return []
            table = all_tables[ind]
        else:
            # Query table by ID
            table = soup.find(id=tableid)
        if table is None:
            return []

        Logger.log(f"Data table found. Extracting data...")
        rows = table.find("tbody").find_all("tr")
        return rows

    def _is_array_field(self, col: str) -> bool:
        """ Check if selected column can hold multiple entries """
        if col.lower() == "title":
            return False
        return True
