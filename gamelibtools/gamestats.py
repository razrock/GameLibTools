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
        self.total_pal = 0
        self.total_jp = 0
        self.total_na = 0
        self.exclusive_pal = 0
        self.exclusive_jp = 0
        self.exclusive_na = 0

    def count_exclusives(self):
        """ Get total number of exclusive games """
        return self.exclusive_pal + self.exclusive_na + self.exclusive_jp