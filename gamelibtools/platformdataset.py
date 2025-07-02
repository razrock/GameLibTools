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
from gamelibtools.util import *

class PlatformDataset:
    """ Platform games dataset """

    def __init__(self):
        """ Class constructor """
        self.games = []
        self.developers = {}
        self.publishers = {}
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
        process_company(self.developers, gameinf.developers)
        process_company(self.publishers, gameinf.publishers)


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
        print(f"Data table exported to {fpath}")


    def report(self):
        """ Print dataset summary & statistics """
        reginf = ''
        excinf = ''
        for reg in self.regions:
            reginf += ('' if len(reginf) == 0 else ' | ') + f"{reg}: {self.stats.get_region_count(reg):4}"
            excinf += ('' if len(excinf) == 0 else ' | ') + f"{reg}: {self.stats.get_exclusives(reg):4}"
        print(f"Total:      {len(self.games):4} | {reginf}")
        print(f"Exclusives: {self.stats.count_exclusives():4} | {excinf}")
        print(f"Developers: {len(self.developers):4}")
        print(f"Publishers: {len(self.publishers):4}")
        if len(self.developers) > 0:
            print(f"\nTop 10 developers by game count:")
            print_company_games(self.developers)
        if len(self.publishers) > 0:
            print(f"\nTop 10 publishers by game count:")
            print_company_games(self.publishers)