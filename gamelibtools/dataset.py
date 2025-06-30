"""
    Game Library Tools
    Games dataset

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
from gamelibtools.gameinfo import *
from gamelibtools.util import *

class GameDataset:
    """ Games dataset """

    def __init__(self):
        """ Class constructor """
        self.games = []
        self.developers = {}
        self.publishers = {}
        self.stats = GameStats()

    def add(self, gameinf):
        """
        Add new game to the dataset
        :param gameinf: Game info
        """
        self.games.append(gameinf)

        # Update stats
        if gameinf.has_regions():
            for platform, pinf in gameinf.platforms.items():
                exc = pinf.is_exclusive()
                if pinf.has_pal:
                    self.stats.total_pal += 1
                    if exc:
                        self.stats.exclusive_pal += 1
                if pinf.has_jp:
                    self.stats.total_jp += 1
                    if exc:
                        self.stats.exclusive_jp += 1
                if pinf.has_na:
                    self.stats.total_na += 1
                    if exc:
                        self.stats.exclusive_na += 1
                break

        # Update companies
        process_company(self.developers, gameinf.get_developers())
        process_company(self.publishers, gameinf.get_publishers())


    def export(self, fpath: str, platform: str, cols: list):
        """
        Export games data table to a CSV file
        :param fpath: File path
        :param platform: Platform name
        :param cols: Data columns
        """
        with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            for row in self.games:
                writer.writerow(row.get_row(platform, cols))
        print(f"Data table exported to {fpath}")

    def report(self):
        """ Print dataset summary & statistics """
        print(f"Total:      {len(self.games):4} / PAL: {self.stats.total_pal:4} / JP: {self.stats.total_jp:4} / NA {self.stats.total_na:4}")
        print(f"Exclusives: {self.stats.count_exclusives():4} / PAL: {self.stats.exclusive_pal:4} / JP: {self.stats.exclusive_jp:4} / NA {self.stats.exclusive_na:4}")
        print(f"Developers: {len(self.developers):4}")
        print(f"Publishers: {len(self.publishers):4}")
        print(f"\nTop 10 developers by game count:")
        print_company_games(self.developers)
        print(f"\nTop 10 publishers by game count:")
        print_company_games(self.publishers)