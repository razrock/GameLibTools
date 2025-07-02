"""
    Game Library Tools
    Game platform info (Game version)

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import json
import dateutil
from dateutil.parser import parse, ParserError
from gamelibtools.util import *


regions = ['NA', 'PAL', 'EU', 'JP', 'KO', 'AU']

class PlatformInfo:
    """ Game platform info (game version) """

    def __init__(self):
        """ Class constructor """
        self.title = ''
        self.aka = []
        self.developers = []
        self.publishers = []
        self.genres = []
        self.release_date = {}
        self.regions = []
        self.flags = []
        self.image_size = 0

    def load(self, data: list, schema: list):
        """
        Load game info from the data row
        :param data: Data row
        :param schema: Data row schema (columns list)
        """
        if data is None or schema is None:
            return
        if len(data) < len(schema):
            print(f"WARNING: Invalid field count: {len(data)}, expected {len(schema)} / {data[0] if len(data) > 0 else '-'}")

        for i in range(0, len(data)):
            if schema[i].lower() == "title":
                if data[i] == "":
                    print("WARNING: Invalid game title")
                    continue
                xlines = data[i].splitlines()
                self.title = xlines[0]
                if len(xlines) > 1:
                    for i in range(1, len(xlines)):
                        if xlines[i] == '':
                            continue
                        self.aka.append(xlines[1])
            elif schema[i].lower() == "developers":
                self.developers = data[i].splitlines()
            elif schema[i].lower() == "publishers":
                self.publishers = data[i].splitlines()
            elif schema[i].lower() == "genre" or schema[i].lower() == "genres":
                self.genres = data[i].splitlines()
            elif schema[i].lower() == "released":
                self._parse_release_date(data[i])
            elif schema[i].lower().startswith("released "):
                reg = schema[i].lower().replace("released ", "").upper()
                self._parse_release_date(data[i], reg)
            elif schema[i].upper() in regions:
                if len(data[i]) > 0:
                    self.regions.append(schema[i].upper())
            elif schema[i].lower() == "flags":
                self.flags = []
                for x in data[i].splitlines():
                    if x:
                        self.flags.append(x)
            else:
                print(f"WARNING: Unknown data field: {schema[i]}")

    def get_row(self, cols: list) -> list:
        """
        Get a data row that includes only the specified columns
        :param cols: Columns list
        :return: Data row
        """
        ret = []
        for col in cols:
            if col.lower() == "title":
                tinf = self.title
                for x in self.aka:
                    tinf += "\r\n" + x
                ret.append(tinf)
            elif col.lower() == "developers":
                ret.append(print_array(self.developers))
            elif col.lower() == "publishers":
                ret.append(print_array(self.publishers))
            elif col.lower() == "genre" or col.lower() == "genres":
                ret.append(print_array(self.genres))
            elif col.lower() == "released":
                rdate = self.get_release_date()
                ret.append(rdate.strftime('%Y-%m-%d') if rdate else None)
            elif col.lower().startswith("released "):
                # Regional release date
                reg = col.lower().replace("released ", "").upper()
                rdate = self.get_region_release_date(reg)
                ret.append(rdate.strftime('%Y-%m-%d') if rdate else None)
            elif col.upper() in self.regions:
                ret.append(self.has_region(col.lower()))
            elif col.lower() == "exclusive":
                ret.append(self.is_exclusive())
            elif col.lower() == "regions":
                ret.append(print_array(self.regions))
            elif col.lower() == "size":
                ret.append(self.image_size)
            elif col.lower() == "flags":
                ret.append(print_array(self.flags))
        return ret

    def resolve_flags(self, fmap: dict):
        """ Resolve flag values """
        nflags = []
        for x in self.flags:
            if x not in fmap:
                print(f"WARNING: Unknown platform data flag - {x}")
            nflags.append(fmap[x])
        self.flags = nflags

    def get_release_date(self):
        """ Calculate game release date """
        if 'WW' in self.release_date:
            return self.release_date['WW']
        ret = None
        for rdate in self.release_date:
            if ret is None or rdate < ret:
                ret = rdate
        return ret

    def get_region_release_date(self, reg: str):
        """ Get a regional release date """
        if reg.upper() == 'WW' or reg == '':
            return self.get_release_date()
        if reg.upper() in self.release_date:
            return self.release_date[reg]
        ret = None
        for xreg in self._get_region_alts(reg.upper()):
            if xreg in self.release_date:
                if ret is None or self.release_date[xreg] < ret:
                    ret = self.release_date[xreg]
        return ret

    def has_region(self, reg: str) -> bool:
        """ Check if game has a regional release """
        if reg.upper() in self.regions:
            return True
        for xreg in self._get_region_alts(reg.upper()):
            if xreg in self.regions:
                return True
        return False

    def has_regions(self) -> bool:
        """ Check if game has regional versions """
        return len(self.regions) > 0

    def is_exclusive(self):
        """ Check if game version is region exclusive """
        return len(self.regions) == 1

    def _get_region_alts(self, reg: str) -> list:
        """ Get alternate region names """
        ret = []
        if reg == 'PAL':
            return ['EU', 'AU']
        if reg == 'AU' or reg == 'EU':
            return ['PAL']
        if reg == 'JP':
            return ['KO']
        if reg == 'KO':
            return ['JP']
        return []

    def _parse_release_date(self, rdate: str, dreg: str='WW'):
        """
        Parse release date info
        :param rdate: Release date or a list of release dates with region designators
        :param dreg: Default / fallback region
        """
        if not rdate or rdate == 'Unreleased' or rdate.startswith("Unreleased"):
            return
        tokens = rdate.splitlines()
        for x in tokens:
            if x == "":
                continue
            reg = dreg
            if x.endswith(")"):
                sx = x.rfind(" (")
                reg = x[sx + 2:-1].upper()
                x = x[0:sx]

            try:
                pdate = dateutil.parser.parse(x)
            except ParserError as ex:
                print(f"WARNING: {ex}")
                dtok = x.split('|')
                if len(dtok) == 3:
                    # Try to parse invalid formats
                    pdate = dateutil.parser.parse(x.replace('|', '/'))
                else:
                    continue
            self.release_date[reg] = pdate

            if reg != 'WW' and reg not in self.regions:
                if reg not in regions:
                    print("WARNING: Unknown region")
                self.regions.append(reg)
