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
        LVLMSG: '\033[94m'
    }
    PRGC = '\033[95m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    loglevel = LOGLVL[LVLSYS]
    proglevel = LOGLVL[LVLMSG]
    proglevels = LVLMSG
    inprogmode = False
    prevmsg = ''
    context = ''
    flog = None

    @staticmethod
    def log(msg: str, lvl: str=LVLMSG):
        """
        Log message
        :param msg: Message text
        :param lvl: Message level
        """
        if lvl not in Logger.LOGLVL:
            return
        if Logger.flog:
            Logger.flog.write(msg + '\n')
        clvl = Logger.LOGLVL[lvl]
        if Logger.loglevel < clvl or clvl <= 0 or clvl > Logger.LOGLVL[Logger.LVLDBG]:
            return
        backtok = ''
        if Logger.inprogmode:
            for i in range(len(Logger.prevmsg)):
                backtok += '\b' + Logger.ENDC
        if lvl in Logger.LOGCOL:
            print(f"{backtok}{Logger.LOGCOL[lvl]}{Logger.context}{msg}{Logger.ENDC}")
        else:
            print(f"{backtok}{Logger.context}{msg}")
        if Logger.inprogmode:
            print(Logger.prevmsg,end='')

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
    def set_context(msg: str):
        Logger.context = '' if msg == '' else msg + ' - '

    @staticmethod
    def clear_context():
        Logger.set_context('')

    @staticmethod
    def report_progress(msg: str, step: int, total: int):
        """
        Report progress
        :param msg: Message text
        :param step: Current step
        :param total: Total steps
        """
        if Logger.loglevel < Logger.proglevel:
            return
        start = False
        end = False
        if not Logger.inprogmode:
            Logger.inprogmode = True
            start = True
        if step >= total:
            Logger.inprogmode = False
            end = True

        backtok = ''
        for i in range(len(Logger.prevmsg)):
            backtok += '\b'
        endtok = '' if Logger.inprogmode else '\n'
        proc = int(100.0 * float(step) / float(total)) if total > 0 else 0

        txt = ''
        txt += f"{Logger.PRGC}" if start else ''
        txt += f"{backtok}{Logger.context}{msg} {step} / {total} ({proc}%)..."
        txt += f"{Logger.ENDC}" if end else ''

        Logger.prevmsg = txt
        print(txt, end=endtok)

    @staticmethod
    def open_flog(fname: str):
        """ Open file for logging """
        Logger.flog = open(fname, "w")

    @staticmethod
    def save_flog():
        """ Open file for logging """
        Logger.log('')
        Logger.flog.close()
        Logger.flog = None

    @staticmethod
    def set_level(lvl: str):
        """ Set logging level """
        if lvl not in Logger.LOGLVL:
            return
        Logger.loglevel = Logger.LOGLVL[lvl]

    @staticmethod
    def set_prog_level(lvl: str):
        """ Set progress reporting level """
        if lvl not in Logger.LOGLVL:
            return
        Logger.proglevel = Logger.LOGLVL[lvl]
        Logger.proglevels = lvl
