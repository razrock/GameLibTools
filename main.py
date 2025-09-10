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

from gamelibtools.igdbsync import *
from gamelibtools.logger import *

def main():
    """ Application entry point """
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-cmd', help='Initial command')
    parser.add_argument('-datadir', help='Data directory', default='data')
    args = parser.parse_args()

    Logger.set_level(Logger.LVLMSG)
    # =============================================================================================
    # Supported commands
    #
    # sync                      Sync database
    # stats                     Print DB stats
    # quit                      Exit program
    #
    # =============================================================================================

    # Command processor loop
    try:
        datamgr = IgdbSync(args.datadir)
        datamgr.load()
        while True:
            cmd = input("IGDB :> ")
            match cmd:
                case 'sync':
                    datamgr.sync()
                case 'stats':
                    datamgr.calc_stats()
                case 'quit':
                    break
                case _:
                    print(f"Unknown command ({cmd}). Please try again")
    except Exception as conerr:
        print('Error occurred: ' + conerr.__str__())
        sys.exit(1)


if __name__ == '__main__':
    main()
