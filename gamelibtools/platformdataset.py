"""
    Game Library Tools
    Platform games dataset

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import csv
from gamelibtools.gamestats import *
from gamelibtools.logger import Logger
from gamelibtools.util import *

class PlatformDataset:
    """ Platform games dataset """

    def __init__(self):
        """ Class constructor """
        self.games = []
        self.developers = {}
        self.publishers = {}
        self.genres = {}
        self.regions = []
        self.stats = GameStats()

    def add(self, gameinf):
        """
        Add new game to the dataset
        :param gameinf: Game info
        """
        self.games.append(gameinf)

        # Update stats
        if gameinf.has_regions():
            for reg in gameinf.regions:
                self.stats.add_region(reg)
                if reg not in self.regions:
                    self.regions.append(reg)
            if gameinf.is_exclusive():
                self.stats.add_exclusive(gameinf.regions[0])

        # Update companies
        process_stat_list(self.developers, gameinf.developers)
        process_stat_list(self.publishers, gameinf.publishers)
        process_stat_list(self.genres, gameinf.genres)


    def export(self, fpath: str, cols: list):
        """
        Export games data table to a CSV file
        :param fpath: File path
        :param cols: Data columns
        """
        with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            for row in self.games:
                writer.writerow(row.get_row(cols))
        Logger.log(f"Data table exported to {fpath}")


    def report(self):
        """ Print dataset summary & statistics """
        reginf = ''
        excinf = ''
        for reg in self.regions:
            reginf += ('' if len(reginf) == 0 else ' | ') + f"{reg}: {self.stats.get_region_count(reg):4}"
            excinf += ('' if len(excinf) == 0 else ' | ') + f"{reg}: {self.stats.get_exclusives(reg):4}"
        Logger.log(f"Total:      {len(self.games):4} | {reginf}")
        Logger.log(f"Exclusives: {self.stats.count_exclusives():4} | {excinf}")
        if len(self.developers) > 0:
            Logger.log(f"Developers: {len(self.developers):4}")
        if len(self.publishers) > 0:
            Logger.log(f"Publishers: {len(self.publishers):4}")
        if len(self.genres) > 0:
            Logger.log(f"Genres    : {len(self.genres):4}")

        if len(self.developers) > 0:
            Logger.log(f"\nTop 10 developers by game count:")
            print_stat(self.developers)
        if len(self.publishers) > 0:
            Logger.log(f"\nTop 10 publishers by game count:")
            print_stat(self.publishers)
        if len(self.genres) > 0:
            Logger.log(f"\nTop 10 genres by game count:")
            print_stat(self.genres)