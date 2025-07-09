"""
    Game Library Tools
    System logger

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""


class Logger:
    """ System logger """
    LVLNONE = ''
    LVLERR = 'ERR'
    LVLSYS = 'SYS'
    LVLWRN = 'WRN'
    LVLMSG = 'MSG'
    LVLDBG = 'DBG'
    LOGLVL = {
        LVLNONE: 0,
        LVLERR: 1,
        LVLSYS: 2,
        LVLWRN: 2,
        LVLMSG: 3,
        LVLDBG: 4
    }
    LOGCOL = {
        LVLERR: '\033[91m',
        LVLSYS: '\033[92m',
        LVLWRN: '\033[93m',
        LVLDBG: '\033[94m'
    }
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    loglevel = LOGLVL[LVLSYS]

    @staticmethod
    def log(msg: str, lvl: str=LVLMSG):
        """
        Log message
        :param msg: Message text
        :param lvl: Message level
        """
        if lvl not in Logger.LOGLVL:
            return
        clvl = Logger.LOGLVL[lvl]
        if Logger.loglevel < clvl or clvl <= 0 or clvl > Logger.LOGLVL[Logger.LVLDBG]:
            return
        if lvl in Logger.LOGCOL:
            print(f"{Logger.LOGCOL[lvl]}{msg}{Logger.ENDC}")
        else:
            print(f"{msg}")

    @staticmethod
    def sysmsg(msg: str):
        Logger.log(msg, Logger.LVLSYS)

    @staticmethod
    def error(msg: str):
        Logger.log(msg, Logger.LVLERR)

    @staticmethod
    def warning(msg: str):
        Logger.log(msg, Logger.LVLWRN)

    @staticmethod
    def dbgmsg(msg: str):
        Logger.log(msg, Logger.LVLDBG)

    @staticmethod
    def set_level(lvl: str):
        """ Set logging level """
        if lvl not in Logger.LOGLVL:
            return
        Logger.loglevel = Logger.LOGLVL[lvl]
