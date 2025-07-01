"""
    Game Library Tools
    Game platform info (Game version)

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import json
from dateutil.parser import parse
from gamelibtools.util import *


class PlatformInfo:
    """ Game platform info (game version) """

    def __init__(self):
        """ Class constructor """
        self.title = ''
        self.developers = []
        self.publishers = []
        self.genres = []
        self.release_date = None
        self.release_date_pal = None
        self.release_date_jp = None
        self.release_date_na = None
        self.has_pal = False
        self.has_jp = False
        self.has_na = False
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
            print(f"Invalid data entry. Invalid field count: {len(data)}, expected {len(schema)} / {data[0] if len(data) > 0 else '-'}")

        pinf = PlatformInfo()
        for i in range(0, len(data)):
            if schema[i].lower() == "title":
                self.title = data[i]
            elif schema[i].lower() == "developers":
                self.developers = data[i].splitlines()
            elif schema[i].lower() == "publishers":
                self.publishers = data[i].splitlines()
            elif schema[i].lower() == "genre" or schema[i].lower() == "genres":
                self.genres = data[i].splitlines()
            elif schema[i].lower() == "released":
                self.release_date = parse_release_date(data[i])
            elif schema[i].lower() == "released pal":
                self.release_date_pal = parse_release_date(data[i])
            elif schema[i].lower() == "released jp":
                self.release_date_jp = parse_release_date(data[i])
            elif schema[i].lower() == "released na":
                self.release_date_na = parse_release_date(data[i])
            elif schema[i].lower() == "pal":
                self.has_pal = len(data[i]) > 0
            elif schema[i].lower() == "jp":
                self.has_jp = len(data[i]) > 0
            elif schema[i].lower() == "na":
                self.has_na = len(data[i]) > 0

        # Check regions & exclusivity
        if not pinf.has_pal and self.release_date_pal is not None:
            self.has_pal = True
        if not pinf.has_jp and self.release_date_jp is not None:
            self.has_jp = True
        if not pinf.has_na and self.release_date_na is not None:
            self.has_na = True

    def get_row(self, cols: list) -> list:
        """
        Get a data row that includes only the specified columns
        :param cols: Columns list
        :return: Data row
        """
        ret = []
        for col in cols:
            if col.lower() == "title":
                ret.append(self.title)
            elif col.lower() == "developers":
                ret.append(str(self.developers))
            elif col.lower() == "publishers":
                ret.append(str(self.publishers))
            elif col.lower() == "genre" or col.lower() == "genres":
                ret.append(str(self.genres))
            elif col.lower() == "released":
                ret.append(self.release_date.strftime('%Y-%m-%d') if self.release_date else None)
            elif col.lower() == "released pal":
                ret.append(self.release_date_pal.strftime('%Y-%m-%d') if self.release_date_pal else None)
            elif col.lower() == "released jp":
                ret.append(self.release_date_jp.strftime('%Y-%m-%d') if self.release_date_jp else None)
            elif col.lower() == "released na":
                ret.append(self.release_date_na.strftime('%Y-%m-%d') if self.release_date_na else None)
            elif col.lower() == "pal":
                ret.append(self.has_pal)
            elif col.lower() == "jp":
                ret.append(self.has_jp)
            elif col.lower() == "na":
                ret.append(self.has_na)
            elif col.lower() == "regions":
                ret.append(self.has_regions())
            elif col.lower() == "exclusive":
                ret.append(self.is_exclusive())
            elif col.lower() == "size":
                ret.append(self.image_size)
        return ret

    def to_json(self) -> str:
        """ Serialize object defintion to JSON """
        xmap = {
            'title': self.title,
            'developers': self.developers,
            'publishers': self.publishers,
            'genres': self.genres,
            'released': self.release_date.strftime('%Y-%m-%d') if self.release_date else None,
            'regions': self.has_regions,
            'exclusive': self.is_exclusive,
            'size': self.image_size
        }
        if self.has_regions:
            xmap['pal'] = self.has_pal
            xmap['jp'] = self.has_jp
            xmap['na'] = self.has_na
            if self.release_date_pal:
                xmap['releasedpal'] = self.release_date_pal.strftime('%Y-%m-%d')
            if self.release_date_pal:
                xmap['releasedjp'] = self.release_date_jp.strftime('%Y-%m-%d')
            if self.release_date_pal:
                xmap['releasedna'] = self.release_date_na.strftime('%Y-%m-%d')
        return json.dumps(xmap)

    def from_json(self, desc: str):
        """ Parse object defintion from JSON string """
        dobj = json.loads(desc)
        if not dobj:
            return
        if 'title' in dobj:
            self.title = dobj['title']
        if 'developers' in dobj:
            self.developers = dobj['developers']
        if 'publishers' in dobj:
            self.publishers = dobj['publishers']
        if 'genres' in dobj:
            self.genres = dobj['genres']
        if 'released' in dobj:
            self.release_date = parse(dobj['released'])
        if 'releasedpal' in dobj:
            self.release_date_pal = parse(dobj['releasedpal'])
        if 'releasedjp' in dobj:
            self.release_date_jp = parse(dobj['releasedjp'])
        if 'releasedna' in dobj:
            self.release_date_na = parse(dobj['releasedna'])
        if 'pal' in dobj:
            self.has_pal = dobj['pal']
        if 'jp' in dobj:
            self.has_jp = dobj['jp']
        if 'na' in dobj:
            self.has_na = dobj['na']
        if 'size' in dobj:
            self.image_size = dobj['size']
        return

    def calc_release_date(self):
        """ Calculate game release date """
        if self.release_date_pal and (not self.release_date or self.release_date > self.release_date_pal):
            self.release_date = self.release_date_pal
        if self.release_date_jp and (not self.release_date or self.release_date > self.release_date_jp):
            self.release_date = self.release_date_jp
        if self.release_date_na and (not self.release_date or self.release_date > self.release_date_na):
            self.release_date = self.release_date_na

    def has_regions(self) -> bool:
        """ Check if game has regional versions """
        if self.has_pal or self.has_jp or self.has_na:
            return True
        return False

    def is_exclusive(self):
        """ Check if game version is region exclusive """
        reg = (1 if self.has_pal else 0) + (1 if self.has_jp else 0) + (1 if self.has_na else 0)
        return reg == 1
