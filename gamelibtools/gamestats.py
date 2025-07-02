"""
    Game Library Tools
    Game data statistics

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""


class GameStats:
    """ Game data statistics """

    def __init__(self):
        """ Class constructor """
        self.regions = {}
        self.exclusives = {}

    def add_region(self, reg: str):
        """
        Count regional release
        :param reg: Region
        """
        if reg not in self.regions:
            self.regions[reg] = 0
        self.regions[reg] += 1

    def add_exclusive(self, reg: str):
        """
        Count exclusive game
        :param reg: Region
        """
        if reg not in self.exclusives:
            self.exclusives[reg] = 0
        self.exclusives[reg] += 1

    def count_exclusives(self) -> int:
        """ Get total number of exclusive games """
        ret = 0
        for x in self.exclusives:
            ret += self.exclusives[x]
        return ret

    def get_region_count(self, reg: str) -> int:
        """ Get regional releases count """
        if reg in self.regions:
            return self.regions[reg]
        return 0

    def get_exclusives(self, reg: str) -> int:
        """ Get regional exclusives count """
        if reg in self.exclusives:
            return self.exclusives[reg]
        return 0