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
    # import game [id]          Import game data
    # import artwork [id]       Import game artwork
    # import screenshots [id]   Import game screenshots
    # import platform [name]    Import game data for the entire platform
    # quit                      Exit program
    #
    # =============================================================================================

    # Command processor loop
    cmd = args.cmd.lower() if args.cmd else ''
    try:
        datamgr = IgdbSync(args.datadir)
        datamgr.load()
        while True:
            cmd = input("IGDB :> ").lower() if cmd == '' else cmd
            if cmd == 'sync':
                datamgr.sync()
            elif cmd == 'stats':
                datamgr.calc_stats()
            elif cmd == 'quit':
                break
            elif cmd.startswith('import game '):
                gid = int(cmd.replace('import game ', ''))
                datamgr.import_game(gid)
            elif cmd.startswith('import artwork '):
                gid = int(cmd.replace('import artwork ', ''))
                datamgr.import_artwork(gid)
            elif cmd.startswith('import screenshots '):
                gid = int(cmd.replace('import screenshots ', ''))
                datamgr.import_screenshots(gid)
            elif cmd.startswith('import platform '):
                tok = cmd.replace('import platform ', '')
                pname = int(tok) if tok.isnumeric() else tok
                datamgr.import_platform_games(pname)
            else:
                print(f"Unknown command ({cmd}). Please try again")
            cmd = ''
    except Exception as conerr:
        print('Error occurred: ' + conerr.__str__())
        sys.exit(1)


if __name__ == '__main__':
    main()
