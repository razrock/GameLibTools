"""
    Game Library Tools
    IGDB sync manager

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import copy
import datetime
import json
import os
from types import NoneType

from gamelibtools.dataset import DataSet
from gamelibtools.datatable import DataTable
from gamelibtools.igdbclient import IgdbClient
from gamelibtools.util import *

class IgdbSync:
    """ IGDB data / sync manager """

    def __init__(self, datapath: str):
        """ Class constructor """
        self.config_dir = './config'
        self.log_dir = './logs'
        self.data_dir = datapath if datapath else './data'
        self.screenshot_dir = self.data_dir + '/screenshots'
        self.covers_dir = self.data_dir + '/covers'
        self.artwork_dir = self.data_dir + '/artwork'
        self.gamecards_dir = self.data_dir + '/gamecards'
        self.gameindex_dir = self.data_dir + '/gameindex'
        self.platform_stats_file = 'igdb_platform_stats.csv'
        self.apiclient = IgdbClient()
        self.dataset = DataSet(self.apiclient, self.data_dir, self.config_dir)
        self.games_manifest = DataTable("games_manifest", "Games manifest", f"{self.data_dir}/igdb_games_manifest.csv", '/games', self.dataset.sources['games_manifest']['schema'])
        self.games_plaforms_index = {}
        self.games_plaforms_index_cols = ['id', 'name', 'game_type', 'release_dates', 'genres', 'metascore', 'rating']
        self.isloaded = False

        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.screenshot_dir):
            os.makedirs(self.screenshot_dir)
        if not os.path.exists(self.covers_dir):
            os.makedirs(self.covers_dir)
        if not os.path.exists(self.artwork_dir):
            os.makedirs(self.artwork_dir)
        if not os.path.exists(self.gamecards_dir):
            os.makedirs(self.gamecards_dir)
        if not os.path.exists(self.gameindex_dir):
            os.makedirs(self.gameindex_dir)

    def load(self):
        """ Load all data tables """
        self.dataset.load()
        if self.isloaded:
            return

        Logger.sysmsg(f"Loading games manifest...")
        self.dataset.load_table(self.games_manifest, self._proc_game_manifest_row)

        # Load platform indices
        Logger.sysmsg(f"Loading platform indices...")
        customplatf = 'platforms' in self.dataset.sources['platform_index']
        plist = self.dataset.sources['platform_index']['platforms'] if customplatf else self.dataset.get_table('platforms').index
        skipunsorted = not self.dataset.sources['platform_index']['unsorted'] if 'unsorted' in self.dataset.sources['platform_index'] else customplatf
        for pid in plist:
            pinf = self.dataset.get_table('platforms').get_row(pid)
            if not pinf:
                continue
            ndt = DataTable(f"gameindex_{pinf['slug']}", f"Platform -{pinf['name']}- game index", f"{self.gameindex_dir}/gameindex_{pinf['slug']}.csv", '', self.dataset.sources['platform_index']['schema'])
            self.games_plaforms_index[pid] = ndt
        if not skipunsorted:
            ndt = DataTable(f"gameindex_unsorted", f"Unsorted games index", f"{self.gameindex_dir}/gameindex_unsorted.csv", '', self.dataset.sources['platform_index']['schema'])
            self.games_plaforms_index[0] = ndt

        isfullind = True
        for pid in self.games_plaforms_index:
            if self.games_plaforms_index[pid].has_file():
                self.games_plaforms_index[pid].load()
            else:
                isfullind = False
        if not isfullind:
            self._index_platform_games(skipunsorted)

        self.isloaded = True

    def sync(self):
        """ Sync all data tables """
        if not self.isloaded:
            self.load()
        self.dataset.sync()

        # Sync game manifest -> Update platform indices
        self.dataset.sync_table(self.games_manifest, self._proc_game_diff)

        # Save changes
        if not self.games_manifest.issaved:
            self.games_manifest.save()
        for pid in self.games_plaforms_index:
            if not self.games_plaforms_index[pid].issaved:
                self.games_plaforms_index[pid].save()
        self.dataset.save()

    def import_game(self, gid: int, loadscreenshots: bool = True, loadartwork: bool = True, overwrite: bool = False):
        """ Import and store game card """
        Logger.log(f"Importing game card for game ID: {gid}")
        if not self.isloaded:
            Logger.warning(f"Unable to import game data. Games manifest not loaed")
            return
        if gid not in self.games_manifest.index:
            Logger.warning(f"Skipping - Invalid game ID: {gid}")
            return

        # Check if game card is already downloaded
        gameinf = copy.deepcopy(self.games_manifest.get_row(gid))
        Logger.set_context(f"{gid}: {gameinf['name']}")
        fpath = self.gamecards_dir + f'/{gid:06}_{gameinf['slug']}.json'
        if not overwrite and os.path.exists(fpath):
            Logger.dbgmsg(f"Game card already downloaded")
            Logger.clear_context()
            return

        # Get data from IGDB
        Logger.dbgmsg(f"Fetching game info...")
        resp = self.apiclient.req(self.games_manifest.backend, f'fields *; exclude {self.games_manifest.get_fields()}; where id = {gid};')
        if not resp or len(resp) == 0:
            Logger.warning(f"No game available on IGDB")
            Logger.clear_context()
            return

        # Resolve references / Composite data
        Logger.dbgmsg(f"Resolving references...")
        self._proc_game_row(resp[0], gameinf, self.dataset.sources['games']['schema'], loadscreenshots, loadartwork)

        # Save game card
        Logger.dbgmsg(f"Saving game card...")
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(gameinf, f,  indent=4)
        Logger.log(f"Game card for '{gameinf['name']}' saved to '{fpath}'")
        Logger.clear_context()

    def import_screenshots(self, gid: int):
        """ Load game screenshots """
        self._import_game_images(gid, 'screenshots', 'screenshots', 'screenshots', self.screenshot_dir, 'screenshot')

    def import_artwork(self, gid: int):
        """ Load game artwork """
        self._import_game_images(gid, 'artwork images', 'artworks', 'artworks', self.artwork_dir, 'artwork')

    def calc_stats(self):
        """ Calculate statistics """
        def proc_game_stats(stats: dict, sid: int|str, iname: str, gameinf: dict, skiptypecount: bool = False):
            if sid in stats:
                stats[sid]['total'] += 1
            else:
                stats[sid] = { 'name': iname, 'total': 1, 'active': 0, 'games': 0, 'exp': 0, 'remakes': 0, 'bundles': 0 }
            if 'game_status' not in gameinf or gameinf['game_status'] == '' or gameinf['game_status'] == 'Released':
                stats[sid]['active'] += 1
            if 'game_type' in gameinf and not skiptypecount:
                if gameinf['game_type'] == 'Main Game':
                    stats[sid]['games'] += 1
                elif gameinf['game_type'] == 'Remaster' or gameinf['game_type'] == 'Remake':
                    stats[sid]['remakes'] += 1
                elif gameinf['game_type'] == 'Bundle' or gameinf['game_type'] == 'Expanded Game':
                    stats[sid]['bundles'] += 1
                else:
                    stats[sid]['exp'] += 1

        # Calculate statistics
        platforms_stats = { }
        genre_stats = { }
        game_type_stats = { }
        game_modes_stats = { }
        engine_stats = { }
        year_stats = { }
        Logger.sysmsg(f"Calculating stats...")
        count = 0
        for game in self.games_manifest.data:
            # Platform stats
            if not game['platforms'] or len(game['platforms']) == 0:
                proc_game_stats(platforms_stats, 0, '** Games with no platform data **', game)
            else:
                for pinf in game['platforms']:
                    proc_game_stats(platforms_stats, pinf['id'], pinf['name'], game)

            # Genre stats
            if not game['genres'] or len(game['genres']) == 0:
                proc_game_stats(genre_stats, "0", '** Games with no genre data **', game)
            else:
                for gx in game['genres']:
                    proc_game_stats(genre_stats, gx, gx, game)

            # Engine stats
            if 'game_engines' in game and game['game_engines']:
                gyear = game['year'] if 'year' in game and game['year'] else 0
                for gx in game['game_engines']:
                    if gx['id'] in engine_stats:
                        engine_stats[gx['id']]['count'] += 1
                        if gyear == 0 or gyear < engine_stats[gx['id']]['from']:
                            engine_stats[gx['id']]['from'] = gyear
                        if gyear == 0 or gyear > engine_stats[gx['id']]['to']:
                            engine_stats[gx['id']]['to'] = gyear
                    else:
                        engine_stats[gx['id']] = { 'name': gx['name'], 'count': 1, 'from': gyear, 'to': gyear }

            # Game modes stats
            if 'game_modes' in game and game['game_modes']:
                for gx in game['game_modes']:
                    if gx in game_modes_stats:
                        game_modes_stats[gx] += 1
                    else:
                        game_modes_stats[gx] = 1

            # Year stats
            if 'year' in game and game['year']:
                if game['year'] in year_stats:
                    year_stats[game['year']]['count'] += 1
                else:
                    year_stats[game['year']] = { 'name': str(game['year']), 'count': 1 }
            else:
                if 0 in year_stats:
                    year_stats[0]['count'] += 1
                else:
                    year_stats[0] = { 'name': '-', 'count': 1 }

            # Game release type stats
            if not game['game_type'] or len(game['game_type']) == 0:
                proc_game_stats(game_type_stats, "0", '** Games with no type data **', game, True)
            else:
                proc_game_stats(game_type_stats, game['game_type'], game['game_type'], game, True)

            count += 1
            if count == 1 or count >= self.games_manifest.count() or count % 1000 == 0:
                Logger.report_progress(f"Processing games", count, self.games_manifest.count())

        # Write platform statistics
        Logger.open_flog(f'{self.log_dir}/stats_platforms.txt')
        Logger.log("Number of games per platform\n")
        Logger.log("  id  |                         name                          |  total | active |  games | remak | bundl |  exp  ")
        Logger.log("=================================================================================================================")
        for stat in sorted(platforms_stats.items(), key=lambda x: x[1]['total'], reverse=True):
            Logger.log(f" {stat[0]:4}  {stat[1]['name']:55}| {stat[1]['total']:6} | {stat[1]['active']:6} | {stat[1]['games']:6} | {stat[1]['remakes']:5} | {stat[1]['bundles']:5} | {stat[1]['exp']:5}")
        Logger.save_flog()

        # Write company statistics / Developers
        lx = sorted(self.dataset.get_table('companies').data, key=lambda x: x['developed'] if x['developed'] else 0, reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_developers.txt')
        Logger.log("Number of games per company\n")
        Logger.log("  no  |                         name                          |          country          |   id   |  status  | count ")
        Logger.log("======================================================================================================================")
        for i in range(100):
            xstatus = lx[i]['status'] if len(lx[i]['status']) > 0 else 'Active'
            Logger.log(f" {(i + 1):4} | {lx[i]['name']:54}| {lx[i]['country']:25} | {lx[i]['id']:6} | {xstatus:8} | {lx[i]['developed']:5}")
        Logger.save_flog()

        # Write company statistics / Publishers
        lx = sorted(self.dataset.get_table('companies').data, key=lambda x: x['published'] if x['published'] else 0, reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_publishers.txt')
        Logger.log("Number of games per company\n")
        Logger.log("  no  |                         name                          |          country          |   id   |  status  | count ")
        Logger.log("======================================================================================================================")
        for i in range(100):
            xstatus = lx[i]['status'] if len(lx[i]['status']) > 0 else 'Active'
            Logger.log(f" {(i + 1):4} | {lx[i]['name']:54}| {lx[i]['country']:25} | {lx[i]['id']:6} | {xstatus:8} | {lx[i]['published']:5}")
        Logger.save_flog()

        # Write year stats
        i = 1
        Logger.open_flog(f'{self.log_dir}/stats_years.txt')
        Logger.log("Games per release year\n")
        Logger.log("  no  | year |  count ")
        Logger.log("======================")
        for year in dict(sorted(year_stats.items(), reverse=True)):
            Logger.log(f" {i:4} | {year_stats[year]['name']:4} | {year_stats[year]['count']:6}")
            i += 1
        Logger.save_flog()

        # Write genre stats
        i = 1
        Logger.open_flog(f'{self.log_dir}/stats_genres.txt')
        Logger.log("Number of games per genre\n")
        Logger.log("  no  |               name                |  total | active |  games | remak | bundl |  exp  ")
        Logger.log("=============================================================================================")
        for stat in genre_stats.items():
            Logger.log(f" {i:4} | {stat[1]['name']:34}| {stat[1]['total']:6} | {stat[1]['active']:6} | {stat[1]['games']:6} | {stat[1]['remakes']:5} | {stat[1]['bundles']:5} | {stat[1]['exp']:5}")
            i += 1
        Logger.save_flog()

        # Write release type stats
        i = 1
        Logger.open_flog(f'{self.log_dir}/stats_game_types.txt')
        Logger.log("Game release types\n")
        Logger.log("  no  |               name                |  total | active ")
        Logger.log("============================================================")
        for stat in game_type_stats.items():
            Logger.log(f" {i:4} | {stat[1]['name']:34}| {stat[1]['total']:6} | {stat[1]['active']:6}")
            i += 1
        Logger.save_flog()

        # Write game engine stats
        i = 1
        Logger.open_flog(f'{self.log_dir}/stats_engines.txt')
        Logger.log("Game engine stats\n")
        Logger.log("  no  |               name                | from |  to  |  count ")
        Logger.log("=================================================================")
        for stat in dict(sorted(engine_stats.items(), key=lambda x: x[1]['count'] if x[1]['count'] else 0, reverse=True)).items():
            if stat[1]['count'] <= 5:
                continue
            Logger.log(f" {i:4} | {stat[1]['name']:34}| {stat[1]['from']:4} | {stat[1]['to']:4} | {stat[1]['count']:6}")
            i += 1
        Logger.save_flog()

        # Write game mode stats
        i = 1
        Logger.open_flog(f'{self.log_dir}/stats_game_modes.txt')
        Logger.log("Game modes stats\n")
        Logger.log("  no  |               name                |  count ")
        Logger.log("=================================================================")
        for gmode in self.dataset.get_table('game_modes').data:
            Logger.log(f" {i:4} | {gmode['name']:34}| {game_modes_stats[gmode['name']]:6}")
            i += 1
        Logger.save_flog()

        # Write franchise stats
        lx = sorted(self.dataset.get_table('franchises').data, key=lambda x: x['count'] if x['count'] else 0, reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_franchises.txt')
        Logger.log("100 biggest game franchises\n")
        Logger.log("  no  |                         name                          |   id   | count ")
        Logger.log("===============================================================================")
        for i in range(100):
            Logger.log(f" {(i + 1):4} | {lx[i]['name']:54}| {lx[i]['id']:6} | {lx[i]['count']:5}")
        Logger.save_flog()

        # Write collection stats
        lx = sorted(self.dataset.get_table('collections').data, key=lambda x: x['count'] if x['count'] else 0, reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_collections.txt')
        Logger.log("100 biggest game collections\n")
        Logger.log("  no  |                         name                          |   id   | count ")
        Logger.log("===============================================================================")
        for i in range(100):
            Logger.log(f" {(i + 1):4} | {lx[i]['name']:54}| {lx[i]['id']:6} | {lx[i]['count']:5}")
        Logger.save_flog()

        # Write top 100 games by rating
        lx = [gmx for gmx in self.games_manifest.data if gmx['rating'] and gmx['metascore'] and gmx['rating'] >= 60 and gmx['metascore'] >= 60 and gmx['game_type'] == 'Main Game']
        lx = sorted(lx, key=lambda x: x['rating'] * 0.52 + x['metascore'] * 0.48, reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_top_rating.txt')
        Logger.log("Top 100 rated games\n")
        Logger.log("  no  |                            name                             | ratg | meta | totl ")
        Logger.log("=========================================================================================")
        for i in range(100):
            tot = int(lx[i]['rating'] * 0.52 + lx[i]['metascore'] * 0.48)
            Logger.log(f" {(i + 1):4} | {lx[i]['name']:60}| {lx[i]['rating']:4} | {lx[i]['metascore']:4} | {tot:4}")
        Logger.save_flog()

        # Write top 100 games by playtime
        lx = [x for x in self.dataset.get_table('game_time_to_beats').data if x['hastily'] and x['normally'] and x['normally'] >= x['hastily'] > 10000 and (not x['completely'] or x['completely'] >= x['normally'])]
        lx = sorted(lx, key=lambda x: x['normally'], reverse=True)[:100]
        Logger.open_flog(f'{self.log_dir}/stats_top_playtime.txt')
        Logger.log("Top 100 games by playtime\n")
        Logger.log("  no  |                               name                                |  normal  |   fast   |   100%   ")
        Logger.log("===========================================================================================================")
        for i in range(100):
            ginf = self.games_manifest.get_row(lx[i]['game_id'])
            if not ginf:
                continue
            tc = seconds_to_hours(lx[i]['completely'] if lx[i]['completely'] else 0)
            tn = seconds_to_hours(lx[i]['normally'] if lx[i]['normally'] else 0)
            th = seconds_to_hours(lx[i]['hastily'] if lx[i]['hastily'] else 0)
            tcs = f"{tc:7.1f}h" if tc > 0 else '-'
            Logger.log(f" {(i + 1):4} | {ginf['name']:66}| {tn:7.1f}h | {th:7.1f}h | {tcs:>8} ")
        Logger.save_flog()

    def _index_platform_games(self, skipunsorted: bool):
        """ Index platform games """
        # Index games
        for gamerow in self.games_manifest.data:
            if 'platforms' in gamerow and gamerow['platforms'] and len(gamerow['platforms']) > 0:
                for pinf in gamerow['platforms']:
                    self.games_plaforms_index[pinf['id']].add_row(gamerow)
            elif not skipunsorted:
                self.games_plaforms_index[0].add_row(gamerow)
            else:
                Logger.dbgmsg(f"No platform data for {gamerow['name']} (ID: {gamerow['id']})")

        # Process & Save data
        for pid in self.games_plaforms_index:
            pinf = self.dataset.get_table('platforms').get_row(pid) if pid > 0 else None
            total = len(self.games_plaforms_index[pid].data)
            for ind in range(total):
                self.games_plaforms_index[pid].data[ind] = self._proc_game_platform_index_row(self.games_plaforms_index[pid].data[ind],pinf)
                if total > 0 and (ind == 0 or ind == total - 1 or ind % 500 == 0):
                    Logger.report_progress("Processing game entry", ind + 1, total)
            self.games_plaforms_index[pid].save()

    def _import_game_images(self, gid: int, iname: str, prop: str, dtable: str, imgdir: str, fpref: str):
        """ Import / load game related images """
        Logger.log(f"Importing game {iname} for game ID: {gid}")
        gameinf = self.games_manifest.get_row(gid)
        if gameinf is None:
            Logger.error(f"Unable to load game {iname}. Invalid game ID")
            return

        Logger.set_context(f"{gid}: {gameinf['name']}")
        Logger.dbgmsg("Game manifest found. Searching for game card...")
        fpath = self.gamecards_dir + f'/{gid:06}_{gameinf['slug']}.json'
        if os.path.exists(fpath):
            Logger.dbgmsg(f"Game card '{fpath}' found. Loading data...")
            gamedata = json.load(open(fpath, 'r', encoding='utf-8'))
            if not gamedata:
                Logger.error(f"Unable to load game {iname}. Game card '{fpath}' is corrupted")
                Logger.clear_context()
                return
            if prop not in gamedata or not gamedata[prop]:
                Logger.error(f"Unable to load game {iname}. No images defined for the specified game")
                Logger.clear_context()
                return
            Logger.dbgmsg(f"Game card '{fpath}' loaded. Downloading images...")
            cnt = 0
            for imginf in gamedata[prop]:
                if not os.path.exists(imginf['path']):
                    download_file(imginf['path'], imginf['url'])
                    cnt += 1
            Logger.clear_context()
            Logger.log(f"{cnt} {iname} imported for game '{gameinf['name']}'")
        else:
            # Obtain image data from IGDB
            resp = self.apiclient.req(self.games_manifest.backend, f'fields {prop}; where id = {gid};')
            if resp is None or len(resp) == 0 or prop not in resp[0] or not resp[0][prop] or len(resp[0][prop]) == 0:
                Logger.error("Unable to load game {iname}. No images defined for the specified game")
                Logger.clear_context()
                return
            cnt = len(resp[0][prop])
            self._resolve_game_images(gid, resp[0][prop], dtable, imgdir, fpref, True)
            Logger.clear_context()
            Logger.log(f"{cnt} {iname} imported for game '{gameinf['name']}'")

    def _proc_game_manifest_row(self, srcrow: dict, dstrow: dict|None, schema: list, tkey: str = '') -> dict:
        """ Resolve games manifest entry references """
        try:
            ret = {}
            cmpproc = False
            for cx in schema:
                srckey = cx['field'] if 'field' in cx else cx['name']
                dstkey = cx['name']
                isproc = 'calc' in cx and len(cx['calc']) > 0
                if srckey not in srcrow and not isproc:
                    continue
                if srckey == 'game_status' or srckey == 'game_type' or srckey == 'genres' or srckey == 'alternative_names' or srckey == 'platforms' or srckey == 'game_engines' or srckey == 'game_modes':
                    ret[dstkey] = self.dataset.resolve_ref(srcrow[srckey], cx['ref'], cx['prop'])
                elif srckey == 'release_dates':
                    np = []
                    for rdinf in self.dataset.resolve_ref(srcrow[srckey], cx['ref'], cx['prop']):
                        ydata = { 'date': rdinf['human'] }
                        if 'release_region' in rdinf:
                            ydata['region'] = self.dataset.resolve_ref(rdinf['release_region'], 'release_date_regions', 'region')
                        if 'status' in rdinf:
                            ydata['status'] = self.dataset.resolve_ref(rdinf['status'], 'release_date_statuses', 'name')
                        if 'platform' in rdinf:
                            ydata['platform'] = self.dataset.resolve_ref(rdinf['platform'], 'platforms', 'name')
                        np.append(ydata)
                    ret[dstkey] = np
                elif srckey == 'year':
                    lsr = ret['release_dates'] if 'release_dates' in ret else (dstrow['release_dates'] if 'release_dates' in dstrow else (srcrow['release_dates'] if 'release_dates' in srcrow else None))
                    if lsr:
                        # Use processed/resolved data
                        infkey = 'date' if 'release_dates' in ret or 'release_dates' in dstrow else 'human'
                        myear = 0
                        for rdinf in lsr:
                            xyear = extract_year(rdinf[infkey])
                            if myear == 0 or xyear < myear:
                                myear = xyear
                        ret[dstkey] = None if myear == 0 else myear
                    else:
                        ret[dstkey] = None
                elif srckey == 'involved_companies':
                    if cmpproc:
                        continue
                    ret['developers'] = []
                    ret['publishers'] = []
                    for xinf in self.dataset.resolve_ref(srcrow[srckey], 'involved_companies', None):
                        yinf = self.dataset.resolve_ref(xinf['company'], 'companies', ['id', 'name'])
                        if 'publisher' in xinf and xinf['publisher']:
                            ret['publishers'].append(yinf)
                        if 'developer' in xinf and xinf['developer']:
                            if 'porting' in xinf and xinf['porting']:
                                yinf['porting'] = True
                            if 'supporting' in xinf and xinf['supporting']:
                                yinf['supporting'] = True
                            ret['developers'].append(yinf)
                        elif ('porting' in xinf and xinf['porting']) or ('supporting' in xinf and xinf['supporting']):
                            if 'porting' in xinf and xinf['porting']:
                                yinf['porting'] = True
                            if 'supporting' in xinf and xinf['supporting']:
                                yinf['supporting'] = True
                            ret['developers'].append(yinf)
                else:
                    ret[dstkey] = srcrow[srckey]
            if dstrow:
                for key, val in ret.items():
                    dstrow[key] = val
            return ret
        except Exception as e:
            Logger.error(f"Game manifest parsing failed, row ID {srcrow['id']}: {srcrow}. {e}")
            raise e

    def _proc_game_diff(self, row: dict, schema: list, tkey: str) -> dict:
        """ Process new game data during sync """
        ret = self._proc_game_manifest_row(row, None, schema, tkey)

        # Remove game from platform indices
        orgrow = self.games_manifest.get_row(row['id'])
        if orgrow:
            if 'platforms' in orgrow and orgrow['platforms'] and len(orgrow['platforms']) > 0:
                for pinf in orgrow['platforms']:
                    self.games_plaforms_index[pinf['id']].remove_row(row['id'])
            elif 0 in self.games_plaforms_index:
                self.games_plaforms_index[0].remove_row(row['id'])

        # Add game to platform indices
        if 'platforms' in ret and ret['platforms'] and len(ret['platforms']) > 0:
            for pinf in ret['platforms']:
                x = self._proc_game_platform_index_row(ret, pinf)
                self.games_plaforms_index[pinf['id']].add_row(x)
        elif 0 in self.games_plaforms_index:
            x = self._proc_game_platform_index_row(ret, None)
            self.games_plaforms_index[0].add_row(x)
        return ret

    def _proc_game_platform_index_row(self, row: dict, pinf: dict|None) -> dict:
        """ Process games platform index row """
        ret = DataTable.extract_fields(row, self.games_plaforms_index_cols)
        if 'release_dates' in row and row['release_dates']:
            rx = []
            relyear = 0
            for reldinf in row['release_dates']:
                rdyear = extract_year(reldinf['date'])
                if relyear == 0 or rdyear < relyear:
                    relyear = rdyear
                validplatf = (not pinf and 'platform' not in reldinf) or (pinf and 'platform' in reldinf and reldinf['platform'] == pinf['name'])
                if not validplatf:
                    continue
                if 'platform' in reldinf:
                    reldinf.pop('platform')
                rx.append(reldinf)
            ret['release_dates'] = rx
            ret['year'] = relyear if relyear > 0 else None
        return ret

    def _proc_game_row(self, srvrow: dict, locrow: dict, schema: list, loadscreenshots: bool = True, loadartwork: bool = True):
        """ Process game table row """
        try:
            drefs = ['player_perspectives', 'keywords', 'themes', 'collections', 'websites', 'language_supports', 'external_games']
            grefs = ['parent_game', 'similar_games', 'bundles', 'dlcs', 'expanded_games', 'expansions', 'forks', 'ports', 'remakes', 'remasters', 'standalone_expansions', 'version_parent']
            prefs = ['time_normal', 'time_minimal', 'time_full', 'time_count']
            for cx in schema:
                srckey = cx['field'] if 'field' in cx else cx['name']
                dstkey = cx['name']
                isproc = 'calc' in cx and len(cx['calc']) > 0
                if srckey not in srvrow and not isproc:
                    continue

                if srckey in drefs:
                    locrow[dstkey] = self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop'])
                elif srckey in grefs:
                    locrow[dstkey] = self._resolve_game_ref(srvrow[srckey])
                elif srckey in prefs:
                    ptinf = self.dataset.get_table(cx['ref']).find_row(cx['calc'], srvrow['id'])
                    if ptinf:
                        locrow[dstkey] = seconds_to_hours(ptinf[cx['prop']]) if cx['type'] == 'float' and ptinf[cx['prop']] else ptinf[cx['prop']]
                elif srckey == 'first_release_date':
                    locrow[dstkey] = datetime.datetime.fromtimestamp(srvrow[srckey]).strftime("%Y-%m-%d") if srvrow[srckey] > 0 else None
                elif srckey == 'franchises':
                    rx = []
                    if 'franchise' in srvrow:
                        rx.append(self.dataset.resolve_ref(srvrow['franchise'], cx['ref'], cx['prop']))
                    if 'franchises' in srvrow:
                        rx = rx + self.dataset.resolve_ref(srvrow['franchises'], cx['ref'], cx['prop'])
                    locrow[dstkey] = rx
                elif srckey == 'game_localizations':
                    locrow[dstkey] = self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop'])
                    for x in locrow[dstkey]:
                        x['region'] = self.dataset.resolve_ref(x['region'], 'regions', 'name')
                elif srckey == 'age_ratings':
                    locrow[dstkey] = self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop'])
                    for x in locrow[dstkey]:
                        x['organization'] = self.dataset.resolve_ref(x['organization'], 'age_rating_organizations', 'name')
                        x['rating'] = self.dataset.resolve_ref(x['rating'], 'age_rating_categories', 'rating')
                        x['descriptions'] = self.dataset.resolve_ref(x['descriptions'], 'age_rating_content_descriptions', 'description')
                elif srckey == 'videos':
                    locrow[dstkey] = self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop'])
                    for x in locrow[dstkey]:
                        x['url'] = 'https://www.youtube.com/watch?v=' + x['video_id']
                        x.pop('video_id')
                elif srckey == 'multiplayer_modes':
                    mpdata = self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop'])
                    rx = []
                    for x in mpdata:
                        x['platform'] = self.dataset.resolve_ref(x['platform'], 'platforms', 'name')
                        x.pop('id')
                        x.pop('game')
                        rem = []
                        for key, val in x.items():
                            if type(val) is NoneType:
                                rem.append(key)
                            elif type(val) is int and val == 0:
                                rem.append(key)
                        for key in rem:
                            x.pop(key)
                        rx.append(x)
                    locrow[srckey] = rx
                elif srckey == 'cover':
                    imgurl = 'https:' + self.dataset.resolve_ref(srvrow[srckey], cx['ref'], cx['prop']).replace("/t_thumb/", "/t_original/")
                    imgpath = f"{self.covers_dir}/cover_{locrow['slug']}_{locrow['id']}.jpg"
                    locrow[srckey] = { 'path': imgpath, 'url': imgurl }
                    if not os.path.exists(imgpath):
                        download_file(imgpath, imgurl)
                elif srckey == 'screenshots':
                    locrow[srckey] = self._resolve_game_images(locrow['id'], srvrow[srckey], 'screenshots', self.screenshot_dir, 'screenshot', loadscreenshots)
                elif srckey == 'artworks':
                    locrow[srckey] = self._resolve_game_images(locrow['id'], srvrow[srckey], 'artworks', self.artwork_dir, 'artwork', loadartwork)
                else:
                    locrow[dstkey] = srvrow[srckey]
        except Exception as e:
            Logger.error(f"Game data parsing failed, row ID {srvrow['id']}: {srvrow}. {e}")
            raise e

    def _resolve_game_ref(self, src: list|int) -> list|dict|None:
        """ Resolve game reference(s) """
        if type(src) is list:
            ret = []
            for xid in src:
                if not self.games_manifest.in_index(xid):
                    Logger.warning(f"Error resolving game reference {xid}")
                    continue
                ginf = { 'id': xid, 'name': self.games_manifest.get_row(xid)['name'] }
                ret.append(ginf)
            return ret
        elif type(src) is int:
            if not self.games_manifest.in_index(src):
                Logger.warning(f"Error resolving game reference {src}")
                return None
            return { 'id': src, 'name': self.games_manifest.get_row(src)['name'] }
        else:
            return None

    def _resolve_game_images(self, gid: int, imgids: list, dtable: str, imgdir: str, fpref: str, download: bool) -> list:
        """ Resolve and download game related images """
        rx = []
        cnt = 1
        imginfs = self.dataset.resolve_ref(imgids, dtable, 'url')
        for imginf in imginfs:
            imgurl = 'https:' + imginf.replace("/t_thumb/", "/t_original/")
            imgpath = f"{imgdir}/{fpref}_{gid}_{cnt}.jpg"
            rx.append({ 'path': imgpath, 'url': imgurl })
            if download and not os.path.exists(imgpath):
                download_file(imgpath, imgurl)
            cnt += 1
        return rx

class IgdbSyncL:
    """ IGDB data / sync manager """

    def import_games(self):
        """ Import games using the IGDB sources configuration """
        cpath = self.config_dir + '/igdbsources.json'
        if not os.path.exists(cpath):
            return
        sources = json.load(open(cpath))
        Logger.sysmsg(f"Importing games from the IGDB...")
        if self.games_manifest.count() == 0:
            Logger.warning(f"Aborting operation. Games manifest not loaded...")
            return

        # Index platform games
        reindex = False
        skipunsorted = 'skipunsorted' in sources and sources['skipunsorted']
        for pinf in self.platforms.data:
            fpath = self.gameindex_dir + f'/gameindex_igdb_{pinf['slug']}.csv'
            if not os.path.exists(fpath):
                reindex = True
                break
        if not reindex and not skipunsorted and not os.path.exists(self.gameindex_dir + f'/gameindex_igdb_unsorted.csv'):
            reindex = True
        if reindex:
            self._index_platform_games(skipunsorted)
        else:
            # Load platform indices
            for pinf in self.platforms.data:
                self._load_platform_games(pinf['id'], pinf['name'], pinf['slug'])
            if not skipunsorted:
                self._load_platform_games(0, "Unsorted games", "unsorted")

        # Validating platform sources
        Logger.log(f"Validating IGDB sources...")
        for pid in sources['platforms']:
            pinf = self.platforms.get_row(pid)
            if not pinf:
                Logger.warning(f"Skipping platform {pid}. Invalid platform ID...")
                continue

        # Import game cards
        # TODO: Import game cards from platform index -> Apply filtering, store current ID
