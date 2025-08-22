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

from gamelibtools.logger import Logger
from gamelibtools.util import *


class PlatformInfo:
    """ Game platform info (game version) """
    REGIONS = {
        'PAL': ['EU', 'AU', 'AUS', 'PAL', 'DE', 'GER', 'UK', 'NZ', 'FR', 'ES', 'IT', 'BE', 'NL', 'RU', 'NO', 'IN', 'IE', 'SAR'],
        'NTSC-U': ['NA', 'US'],
        'NTSC-J': ['JP', 'TW', 'KOR', 'KO', 'AS']
    }

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

    def load(self, data: list, schema: list):
        """
        Load game info from the data row
        :param data: Data row
        :param schema: Data row schema (columns list)
        """
        if data is None or schema is None:
            return
        if len(data) < len(schema):
            Logger.warning(f"WARNING: Invalid field count: {len(data)}, expected {len(schema)} / {data[0] if len(data) > 0 else '-'}")

        reg_code_codes = self._get_code_list()
        for i in range(0, len(data)):
            if schema[i].lower() == "title":
                if data[i] == "":
                    Logger.warning("WARNING: Invalid game title")
                    continue
                xlines = data[i].splitlines()
                self.title = xlines[0]
                if len(xlines) > 1:
                    Logger.dbgmsg(f"Multiple titles found for {self.title}")
                    cline = ''
                    for j in range(1, len(xlines)):
                        if xlines[j] == '':
                            if len(cline) > 0:
                                self.aka.append(cline)
                                cline = ''
                        else:
                            cline += xlines[j]
                    if len(cline) > 0:
                        self.aka.append(cline)
            elif schema[i].lower() == "developers":
                self.developers = []
                for x in data[i].splitlines():
                    if x != '':
                        self.developers.append(x)
                if len(self.developers) == 0:
                    Logger.dbgmsg(f"Missing game developers / {self.title}")
            elif schema[i].lower() == "publishers":
                self.publishers = []
                for x in data[i].splitlines():
                    if x != '':
                        self.publishers.append(x)
                if len(self.publishers) == 0:
                    Logger.dbgmsg(f"Missing game publishers / {self.title}")
            elif schema[i].lower() == "genre" or schema[i].lower() == "genres":
                self.genres = data[i].splitlines()
            elif schema[i].lower() == "released":
                self._parse_release_date(data[i])
            elif schema[i].lower().startswith("released "):
                reg = schema[i].lower().replace("released ", "").upper()
                self._parse_release_date(data[i], reg)
            elif schema[i].upper() in reg_code_codes:
                if len(data[i]) > 0:
                    reg = self._get_region_from_code(schema[i].upper())
                    if reg not in self.regions:
                        self.regions.append(reg)
            elif schema[i].lower() == "regions":
                for x in data[i].splitlines():
                    y = x.split(',')
                    for reg in y:
                        fnd_reg = self._get_region_from_code(reg.strip())
                        if fnd_reg not in self.regions:
                            self.regions.append(fnd_reg)
            elif schema[i].lower() == "flags":
                self.flags = []
                for x in data[i].splitlines():
                    if x:
                        self.flags.append(x)
            else:
                Logger.warning(f"WARNING: Unknown data field: {schema[i]} / {self.title}")

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
            elif col.lower() == "release dates":
                tmap = {}
                for rcode in self.release_date:
                    tmap[rcode] = self.release_date[rcode].strftime('%Y-%m-%d')
                ret.append(json.dumps(tmap))
            elif col.upper() in self.regions:
                ret.append(self.has_region(col.lower()))
            elif col.lower() == "exclusive":
                ret.append(self.is_exclusive())
            elif col.lower() == "regions":
                ret.append(print_array(self.regions))
            elif col.lower() == "flags":
                ret.append(print_array(self.flags))
        return ret

    def resolve_flags(self, fmap: dict):
        """ Resolve flag values """
        nflags = []
        for x in self.flags:
            if x not in fmap:
                Logger.warning(f"WARNING: Unknown platform data flag - {x}")
            nflags.append(fmap[x])
        self.flags = nflags

    def get_release_date(self):
        """ Calculate game release date """
        if 'WW' in self.release_date:
            return self.release_date['WW']
        ret = None
        for rdate in self.release_date:
            if ret is None or self.release_date[rdate] < ret:
                ret = self.release_date[rdate]
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
        return reg.upper() in self.regions

    def has_regions(self) -> bool:
        """ Check if game has regional versions """
        return len(self.regions) > 0

    def is_exclusive(self):
        """ Check if game version is region exclusive """
        return len(self.regions) == 1

    def _get_region_alts(self, reg: str) -> list:
        """ Get alternate region names """
        if reg in PlatformInfo.REGIONS:
            return PlatformInfo.REGIONS[reg]
        x = self._get_region_from_code(reg)
        return [] if x == '' else [x]

    def _get_region_from_code(self, reg: str) -> str:
        """ Get region from a country/region code """
        for x in PlatformInfo.REGIONS:
            for y in PlatformInfo.REGIONS[x]:
                if reg == y:
                    return x
        return ""

    def _get_code_list(self) -> list:
        """ Get a list of all country/region codes"""
        ret = []
        for x in PlatformInfo.REGIONS:
            for y in PlatformInfo.REGIONS[x]:
                ret.append(y)
        return ret

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
                Logger.warning(f"WARNING: {ex} / {self.title}")
                sep = '|'
                dtok = x.split('|')
                if len(dtok) == 1:
                    dtok = x.replace('–', '-').split('-')
                    sep = '-'
                if len(dtok) == 1:
                    dtok = x.split('/')
                    sep = '/'
                if len(dtok) == 3:
                    # Try to parse invalid formats
                    pdate = dateutil.parser.parse(x.replace('|', '/'))
                elif len(dtok) == 2:
                    pdate = dateutil.parser.parse(x.replace('–', '-') + sep + '01')
                else:
                    continue

            self.release_date[reg] = pdate
            if reg != 'WW':
                parsed_regs = []
                if ',' in reg:
                    tokens = reg.upper().split(',')
                    for c in tokens:
                        c = c.strip()
                        if c == '':
                            continue
                        fnd_reg = self._get_region_from_code(c)
                        if fnd_reg == '':
                            Logger.warning(f"WARNING: Unknown region code {c} / {reg} / {self.title}")
                        else:
                            parsed_regs.append(fnd_reg)
                else:
                    fnd_reg = self._get_region_from_code(reg.upper())
                    if fnd_reg == '':
                        Logger.warning(f"WARNING: Unknown region code {reg} / {self.title}")
                    else:
                        parsed_regs.append(fnd_reg)

                for x in parsed_regs:
                    if x not in self.regions:
                        self.regions.append(x)
