"""
    Game Library Tools
    Application entry point

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import sys
import argparse

from gamelibtools.igdbclient import IgdbClient
from gamelibtools.wikiimporter import *
from gamelibtools.logger import *

def main():
    """ Application entry point """
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help='Command')
    parser.add_argument('-source', help='Data Source')
    parser.add_argument('-platform', help='Platform ID', type=int, default=0)
    args = parser.parse_args()

    # Command selector
    Logger.set_level(Logger.LVLDBG)
    try:
        if args.command == "import":
            if args.source == "wiki":
                datamgr = WikiImporter()
                datamgr.run()
            elif args.source == "igdb":
                igdbapi = IgdbClient()
                igdbapi.load()
                igdbapi.auth()
                igdbapi.load_support_data()
                if args.platform > 0:
                    igdbapi.import_platform_games(args.platform)
                else:
                    igdbapi.import_games()
            else:
                print(f"Unknown data source ({args.source}). Exiting...")
        else:
            print(f"Unknown command ({args.command}). Exiting...")
    except Exception as conerr:
        print('Error occurred: ' + conerr.__str__())
        sys.exit(1)


if __name__ == '__main__':
    main()
