"""
    Game Library Tools
    Game entry info

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
        self.developers = []
        self.publishers = []
        self.release_date = None
        self.release_date_pal = None
        self.release_date_jp = None
        self.release_date_na = None
        self.has_pal = False
        self.has_jp = False
        self.has_na = False
        self.inventory = []
        self.image_size = 0

    def to_json(self) -> str:
        """ Serialize object defintion to JSON """
        xmap = {
            'developers': self.developers,
            'publishers': self.publishers,
            'released': self.release_date.strftime('%Y-%m-%d') if self.release_date else None,
            'regions': self.has_regions,
            'exclusive': self.is_exclusive,
            'size': self.image_size,
            'inventory': self.inventory,
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

    def parse(self, desc: str):
        """ Parse object defintion from JSON string """
        dobj = json.loads(desc)
        if not dobj:
            return
        if 'developers' in dobj:
            self.developers = dobj['developers']
        if 'publishers' in dobj:
            self.publishers = dobj['publishers']
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
        if 'inventory' in dobj:
            self.inventory = dobj['inventory']
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


class GameInfo:
    """ Game entry info """

    def __init__(self):
        """ Class constructor """
        self.title = ''
        self.genres = []
        self.platforms = {}

    def load_platform(self, platform: str, data: list, schema: list):
        """
        Load game info from the data row
        :param platform: Platform name
        :param data: Data row
        :param schema: Data row schema (columns list)
        """
        if data is None or schema is None:
            return
        if len(data) != len(schema):
            raise f"Invalid data entry. Invalid field count: {len(data)}, expected {len(schema)}"

        pinf = PlatformInfo()
        for i in range(0, len(data)):
            if schema[i].lower() == "title":
                self.title = data[i]
            elif schema[i].lower() == "developers":
                pinf.developers = data[i].splitlines()
            elif schema[i].lower() == "publishers":
                pinf.publishers = data[i].splitlines()
            elif schema[i].lower() == "released":
                pinf.release_date = parse_release_date(data[i])
            elif schema[i].lower() == "released pal":
                pinf.release_date_pal = parse_release_date(data[i])
            elif schema[i].lower() == "released jp":
                pinf.release_date_jp = parse_release_date(data[i])
            elif schema[i].lower() == "released na":
                pinf.release_date_na = parse_release_date(data[i])
            elif schema[i].lower() == "pal":
                pinf.has_pal = len(data[i]) > 0
            elif schema[i].lower() == "jp":
                pinf.has_jp = len(data[i]) > 0
            elif schema[i].lower() == "na":
                pinf.has_na = len(data[i]) > 0

        # Check regions & exclusivity
        if not pinf.has_pal and pinf.release_date_pal is not None:
            pinf.has_pal = True
        if not pinf.has_jp and pinf.release_date_jp is not None:
            pinf.has_na = True
        if not pinf.has_pal and pinf.release_date_na is not None:
            pinf.has_na = True

        self.platforms[platform] = pinf

    def get_row(self, platform: str, cols: list) -> list:
        """
        Get a data row that includes only the specified columns
        :param platform: Platform name
        :param cols: Columns list
        :return: Data row
        """
        ret = []
        for col in cols:
            if col.lower() == "title":
                ret.append(self.title)
            elif col.lower() == "developers":
                ret.append(str(self.platforms[platform].developers))
            elif col.lower() == "publishers":
                ret.append(str(self.platforms[platform].publishers))
            elif col.lower() == "released":
                ret.append(self.platforms[platform].release_date.strftime('%Y-%m-%d') if self.platforms[platform].release_date else None)
            elif col.lower() == "released pal":
                ret.append(self.platforms[platform].release_date_pal.strftime('%Y-%m-%d') if self.platforms[platform].release_date_pal else None)
            elif col.lower() == "released jp":
                ret.append(self.platforms[platform].release_date_jp.strftime('%Y-%m-%d') if self.platforms[platform].release_date_jp else None)
            elif col.lower() == "released na":
                ret.append(self.platforms[platform].release_date_na.strftime('%Y-%m-%d') if self.platforms[platform].release_date_na else None)
            elif col.lower() == "pal":
                ret.append(self.platforms[platform].has_pal)
            elif col.lower() == "jp":
                ret.append(self.platforms[platform].has_jp)
            elif col.lower() == "na":
                ret.append(self.platforms[platform].has_na)
            elif col.lower() == "regions":
                ret.append(self.platforms[platform].has_regions())
            elif col.lower() == "exclusive":
                ret.append(self.platforms[platform].is_exclusive())
            elif col.lower() == "size":
                ret.append(self.platforms[platform].image_size)
        return ret

    def get_total_size(self) -> int:
        """ Get total size of game media """
        ret = 0
        for key, x in self.platforms.items():
            ret += x.image_size
        return ret

    def get_inventory(self) -> list:
        """ Get game inventory (owned media list) """
        ret = []
        for pname, pinf in self.platforms.items():
            for media in pinf.inventory:
                ret.append(pname + ' ' + media)
        return ret

    def get_developers(self) -> list:
        """ Get all associated developers """
        ret = []
        for platform, pinf in self.platforms.items():
            for x in pinf.developers:
                if x not in ret:
                    ret.append(x)
        return ret

    def get_publishers(self) -> list:
        """ Get all associated publishers """
        ret = []
        for platform, pinf in self.platforms.items():
            for x in pinf.publishers:
                if x not in ret:
                    ret.append(x)
        return ret

    def get_release_date(self) -> datetime.datetime:
        """ Get release date """
        release_date = None
        for platform, pinf in self.platforms.items():
            if pinf.release_date and (release_date is None or pinf.release_date < release_date):
                release_date = pinf.release_date
        return release_date

    def has_regions(self) -> bool:
        """ Check if game has regional versions """
        for platform, pinf in self.platforms.items():
            if pinf.has_regions():
                return True
        return False


class GameStats:
    """ Game releated statistics """

    def __init__(self):
        """ Class constructor """
        self.total_pal = 0
        self.total_jp = 0
        self.total_na = 0
        self.exclusive_pal = 0
        self.exclusive_jp = 0
        self.exclusive_na = 0

    def count_exclusives(self):
        """ Get total number of exclusive games """
        return self.exclusive_pal + self.exclusive_na + self.exclusive_jp

