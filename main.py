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

from gamelibtools.wikiimporter import *
from gamelibtools.logger import *

def main():
    """ Application entry point """
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help='Command')
    args = parser.parse_args()

    # Command selector
    Logger.set_level(Logger.LVLMSG)
    try:
        if args.command == "import":
            datamgr = GameImporter()
            datamgr.run()
        else:
            print(f"Unknown command ({args.command}). Exiting...")
    except Exception as conerr:
        print('Error occurred: ' + conerr.__str__())
        sys.exit(1)


if __name__ == '__main__':
    main()
