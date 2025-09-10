"""
    Game Library Tools
    IGDB sync manager

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import datetime
import json
import os
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
        Logger.log(f"Syncing table '{self.games_manifest.name}'...")
        self.dataset.fetch_table(self.games_manifest, self._proc_game_diff, f'where {self.games_manifest.tscol} > {self.games_manifest.lastupdate}')

        # Save changes
        if not self.games_manifest.issaved:
            self.games_manifest.save()
        for pid in self.games_plaforms_index:
            if not self.games_plaforms_index[pid].issaved:
                self.games_plaforms_index[pid].save()
        self.dataset.save()

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

    def _proc_game_manifest_row(self, srcrow: dict, dstrow: dict|None, schema: list, tkey: str) -> dict:
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

    def import_game_screenshots(self, gid: int, loadimg: bool = True) -> list:
        """ Load game screenshots """
        ret = []
        xdata = self._req('/screenshots', f'fields *; limit 500; where game = {gid};')
        cnt = 1
        for ximg in xdata:
            ssurl = "https:" + ximg['url'].replace("/t_thumb/", "/t_original/")
            sspath = f"{self.screenshot_dir}/screenshot_{gid}_{cnt}.jpg"
            if loadimg and not os.path.exists(sspath):
                sspath = download_file(sspath, ssurl)
            if sspath:
                imginf = { 'path': sspath, 'url': ssurl }
                ret.append(imginf)
            cnt += 1
        return ret

    def import_game_artwork(self, gid: int, loadimg: bool = True) -> list:
        """ Load game artwork """
        ret = []
        if 'artwork_types' not in self.support_data or len(self.support_data['artwork_types']) == 0:
            Logger.warning("Unable to load game artwork. Support data not loaded")
            return ret
        xdata = self._req('/artworks', f'fields *; limit 500; where game = {gid};')
        cnt = 1
        for ximg in xdata:
            atype = self.support_data['artwork_types'][ximg['artwork_type']] if 'artwork_type' in ximg and ximg['artwork_type'] in self.support_data['artwork_types'] else None
            awurl = "https:" + ximg['url'].replace("/t_thumb/", "/t_original/")
            awpath = f"{self.artwork_dir}/artwork_{gid}_{cnt}.jpg"
            if loadimg and not os.path.exists(awpath):
                awpath = download_file(awpath, awurl)
            if awpath:
                imginf = { 'path': awpath, 'url': awurl, 'type': atype }
                ret.append(imginf)
            cnt += 1
        return ret

    def import_game(self, gid: int, loadscreenshots: bool = True, loadartwork: bool = True, overwrite: bool = False):
        """ Import and store game card """
        Logger.log(f"Importing game card for game ID: {gid}")
        if gid not in self.games_manifest.index:
            Logger.warning(f"Skipping - Invalid game ID: {gid}")
            return
        gname = self.games_manifest.get_row(gid)['name']
        gslug = self.games_manifest.get_row(gid)['slug']
        Logger.set_context(f"{gid}: {gname}")
        Logger.dbgmsg(f"Game found")
        fpath = self.gamecards_dir + f'/{gid:06}_{gslug}.json'
        if not overwrite and os.path.exists(fpath):
            Logger.dbgmsg(f"Game card already downloaded")
            Logger.clear_context()
            return

        Logger.dbgmsg(f"Fetching game info...")
        resp = self._req('/games', f'fields *; exclude game_status, game_type, alternative_names, release_dates, genres, involved_companies, platforms; where id = {gid};')
        if not resp or len(resp) == 0:
            Logger.warning(f"No game available on IGDB")
            Logger.clear_context()
            return
        Logger.dbgmsg(f"Resolving references...")
        game_inf = self._parse_game(resp[0], True, loadscreenshots, loadartwork)

        Logger.dbgmsg(f"Saving game card...")
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(game_inf, f,  indent=4)
        Logger.dbgmsg(f"Game card for saved to {fpath}")
        Logger.clear_context()

    def _extract_date(self, dt, dformat: str) -> str:
        """ Extract and serialize timestamp """
        if dformat == "YYYY":
            return dt.strftime("%Y")
        if dformat == "YYYYMM":
            return dt.strftime("%Y-%m")
        if dformat == "YYYYQ1":
            return dt.strftime("%Y") + "-02"
        if dformat == "YYYYQ2":
            return dt.strftime("%Y") + "-05"
        if dformat == "YYYYQ3":
            return dt.strftime("%Y") + "-08"
        if dformat == "YYYYQ4":
            return dt.strftime("%Y") + "-11"
        return dt.strftime("%Y-%m-%d")

    def _ref_field(self, src: list, data: list|dict, cols: list = None) -> list:
        """ Resolve field references """
        ret = []
        remap = cols and len(cols) > 0
        if type(data) is list:
            # Resolve references from a data table
            for x in src:
                xinf = next((item for item in data if item["id"] == x), None)
                if not xinf:
                    continue
                if remap and type(data[x]) is dict:
                    ret.append(DataTable.extract_fields(xinf, cols))
                else:
                    ret.append(xinf)
        else:
            # Resolve references from a data map
            for x in src:
                if x in data:
                    if remap and type(data[x]) is dict:
                        ret.append(DataTable.extract_fields(data[x], cols))
                    else:
                        ret.append(data[x])
        return ret

    def _ref_game(self, src: list|int) -> list|dict|None:
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

    def _parse_game(self, data: dict, loadplatforms: bool = False, loadscreenshots: bool = True, loadartwork: bool = True) -> dict:
        """ Parse game data """
        params = ['id', 'name', 'summary', 'storyline' 'aggregated_rating', 'aggregated_rating_count', 'rating', 'rating_count', 'total_rating', 'total_rating_count', 'hypes', 'version_title', 'url', 'slug']
        grefs = ['parent_game', 'similar_games', 'bundles', 'dlcs', 'expanded_games', 'expansions', 'forks', 'ports', 'remakes', 'remasters', 'standalone_expansions', 'version_parent']
        ret = DataTable.extract_fields(data, params)
        if not self.support_data or len(self.support_data) == 0:
            return ret
        ginf = self.games_manifest.get_row(ret['id'])
        if ginf is None:
            return ret

        # Fetch data from the game manifest
        ret['release_dates'] = ginf['release_dates']
        ret['alternative_names'] = ginf['alternative_names']
        ret['developers'] = ginf['developers']
        ret['publishers'] = ginf['publishers']
        ret['genres'] = ginf['genres']
        ret['platforms'] = ginf['platforms']
        ret['game_status'] = ginf['game_status']
        ret['game_type'] = ginf['game_type']

        # Resolve game references
        for col in grefs:
            if col in data:
                ret[col] = self._ref_game(data[col])

        # Resolve data references
        if 'first_release_date' in data and data['first_release_date'] > 0:
            ret['first_release_date'] = datetime.datetime.fromtimestamp(data['first_release_date']).strftime("%Y-%m-%d")
        # if 'game_status' in data and data['game_status'] in self.support_data['game_status']:
        #     ret['game_status'] = self.support_data['game_status'][data['game_status']]
        # if 'game_type' in data and data['game_type'] in self.support_data['game_types']:
        #     ret['game_type'] = self.support_data['game_types'][data['game_type']]
        if 'game_modes' in data:
            ret['game_modes'] = self._ref_field(data['game_modes'], self.support_data['game_modes'])
        # if 'genres' in data:
        #     ret['genres'] = self._ref_field(data['genres'], self.support_data['genres'])
        # if 'release_dates' in data:
        #     ret['release_dates'] = []
        #     for xid in data['release_dates']:
        #         xdata = self._req('/release_dates', f'fields *; limit 500; where id = {xid};')[0]
        #         ydata = { 'date': xdata['human'] }
        #         if 'release_region' in xdata and xdata['release_region'] in self.support_data['release_regions']:
        #             ydata['region'] = self.support_data['release_regions'][xdata['release_region']]
        #         if 'status' in xdata and xdata['status'] in self.support_data['release_status']:
        #             ydata['status'] = self.support_data['release_status'][xdata['status']]
        #         if 'platform' in xdata:
        #             pinf = self.platforms.get_row(xdata['platform'])
        #             if pinf:
        #                 ydata['platform'] = pinf['name']
        #         ret['release_dates'].append(ydata)
        # if 'alternative_names' in data:
        #     xdata = self._req('/alternative_names', f'fields *; limit 500; where game = {ret['id']};')
        #     ret['alternative_names'] = []
        #     for x in xdata:
        #         ret['alternative_names'].append(x['name'])
        # if 'involved_companies' in data:
        #     ret['developers'] = []
        #     ret['publishers'] = []
        #     ret['porting'] = []
        #     ret['support'] = []
        #     for xid in data['involved_companies']:
        #         xdata = self._req('/involved_companies', f'fields *; limit 500; where id = {xid};')[0]
        #         cinf = self.companies.get_row(xdata['company'])
        #         if cinf:
        #             xinf = { 'id': cinf['id'], 'name': cinf['name'] }
        #             if 'developer' in xdata and xdata['developer']:
        #                 ret['developers'].append(xinf)
        #             if 'publisher' in xdata and xdata['publisher']:
        #                 ret['publishers'].append(xinf)
        #             if 'porting' in xdata and xdata['porting']:
        #                 ret['porting'].append(xinf)
        #             if 'supporting' in xdata and xdata['supporting']:
        #                 ret['supporting'].append(xinf)
        #             if len(ret['porting']) == 0:
        #                 ret.pop('porting')
        #             if len(ret['support']) == 0:
        #                 ret.pop('support')
        # if loadplatforms and 'platforms' in data:
        #     ret['platforms'] = []
        #     for pid in data['platforms']:
        #         pinf = self.platforms.get_row(pid)
        #         if not pinf:
        #             continue
        #         ret['platforms'].append(pinf['name'])
        if 'language_supports' in data:
            ret['languages'] = []
            xdata = self._req('/language_supports', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['language_supports']):
                Logger.warning(f"Support languages missmatch for game {ret['name']}")
            for x in xdata:
                lang = self.support_data['languages'][x['language']] if x['language'] in self.support_data['languages'] else None
                ltype = self.support_data['language_support_types'][x['language_support_type']] if x['language_support_type'] in self.support_data['language_support_types'] else None
                ret['languages'].append({ 'lang': lang, 'type': ltype })
        if 'player_perspectives' in data:
            ret['player_perspectives'] = self._ref_field(data['player_perspectives'], self.support_data['player_perspectives'])
        if 'keywords' in data:
            ret['keywords'] = self._ref_field(data['keywords'], self.support_data['keywords'])
        if 'themes' in data:
            ret['themes'] = self._ref_field(data['themes'], self.support_data['themes'])
        if 'game_localizations' in data:
            ret['localizations'] = []
            xdata = self._req('/game_localizations', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['game_localizations']):
                Logger.warning(f"Game localizations missmatch for game {ret['name']}")
            for x in xdata:
                reg = self.support_data['regions'][x['region']]['name'] if 'region' in x and x['region'] in self.support_data['regions'] else None
                ret['localizations'].append({ 'name': x['name'], 'region': reg })
        if 'age_ratings' in data:
            ret['age_ratings'] = []
            for x in data['age_ratings']:
                xdata = self._req('/age_ratings', f'fields *; limit 500; where id = {x};')[0]
                if not xdata:
                    continue
                arinf = {
                    'organization': self.support_data['age_rating_organizations'][xdata['organization']],
                    'rating': self.support_data['age_rating_categories'][xdata['rating_category']]['rating']
                }
                if 'rating_content_descriptions' in xdata:
                    arinf['description'] = []
                    for y in xdata['rating_content_descriptions']:
                        arinf['description'].append(self.support_data['age_rating_descriptions'][y]['description'])
                ret['age_ratings'].append(arinf)
        if 'multiplayer_modes' in data:
            xdata = self._req('/multiplayer_modes', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['multiplayer_modes']):
                Logger.warning(f"Invalid multiplayer modes for game '{ret['name']}'")
            modes = ['campaigncoop', 'dropin', 'lancoop', 'offlinecoop', 'onlinecoop', 'splitscreen', 'splitscreenonline']
            pcounts = ['offlinecoopmax', 'offlinemax', 'onlinecoopmax', 'onlinemax']
            ret['multiplayer_modes'] = []
            for x in xdata:
                pinf = self.platforms.get_row(x['platform']) if 'platform' in x else None
                pname = pinf['name'] if pinf else ''
                mp_platf_inf = { 'platform': pname, 'multiplayer_modes': [] }
                for y in modes:
                    if y in x and x[y]:
                        mp_platf_inf['multiplayer_modes'].append(y)
                for y in pcounts:
                    if y in x:
                        mp_platf_inf[y] = x[y]
                ret['multiplayer_modes'].append(mp_platf_inf)
        if 'game_engines' in data:
            ret['game_engines'] = []
            for x in data['game_engines']:
                xinf = self.engines.get_row(x)
                if xinf:
                    ret['game_engines'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'collections' in data:
            ret['collections'] = []
            for x in data['collections']:
                xinf = self.collections.get_row(x)
                if xinf:
                    ret['collections'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'franchise' in data:
            ret['franchises'] = []
            xinf = self.franchises.get_row(data['franchise'])
            if xinf:
                ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'franchises' in data:
            ret['franchises'] = []
            for x in data['franchises']:
                xinf = self.franchises.get_row(x)
                if xinf:
                    ret['franchises'].append({ 'id': xinf['id'], 'name': xinf['name'], 'url': xinf['url'] })
        if 'cover' in data:
            xdata = self._req('/covers', f'fields *; limit 500; where id = {data['cover']};')[0]
            covurl = "https:" + xdata['url'].replace("/t_thumb/", "/t_original/")
            covpath = f"{self.covers_dir}/cover_{data['slug']}_{ret['id']}.jpg"
            if not os.path.exists(covpath):
                covpath = download_file(covpath, covurl)
            if covpath:
                ret['cover'] = { 'path': covpath, 'url': covurl }
        if 'screenshots' in data:
            ret['screenshots'] = self.import_game_screenshots(ret['id'], loadscreenshots)
        if 'artworks' in data:
            ret['artworks'] = self.import_game_artwork(ret['id'], loadartwork)
        if 'videos' in data:
            ret['videos'] = []
            xdata = self._req('/game_videos', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['videos']):
                Logger.warning(f"Invalid videos for game '{ret['name']}'")
            for x in xdata:
                ret['videos'].append({ 'name': x['name'], 'url': 'https://www.youtube.com/watch?v=' + x['video_id'] })
        if 'websites' in data:
            ret['websites'] = []
            xdata = self._req('/websites', f'fields *; limit 500; where game = {ret['id']};')
            if len(xdata) != len(data['websites']):
                Logger.warning(f"Invalid web sites for game '{ret['name']}'")
            for x in xdata:
                ret['websites'].append(x['url'])
        if 'external_games' in data:
            ret['external_sources'] = []
            xdata = self._req('/external_games', f'fields *; limit 500; where game = {ret['id']};')
            for x in xdata:
                exinf = DataTable.extract_fields(x, ['name', 'url', 'year'])
                if 'countries' in x:
                    exinf['countries'] = []
                    for cid in x['countries']:
                        if str(cid) in self.countries:
                            exinf['countries'].append(self.countries[str(cid)])
                        else:
                            Logger.warning(f"Invalid country reference {cid} for external game source")
                if 'game_release_format' in x and x['game_release_format'] in self.support_data['game_release_formats']:
                    exinf['format'] = self.support_data['game_release_formats'][x['game_release_format']]
                if 'external_game_source' in x and x['external_game_source'] in self.support_data['external_game_sources']:
                    exinf['source'] = self.support_data['external_game_sources'][x['external_game_source']]
                if 'platform' in x:
                    pinf = self.platforms.get_row(x['platform'])
                    if pinf:
                        exinf['platform'] = pinf['name']
                    else:
                        Logger.warning(f"Invalid platform reference {x['platform']} for external game source")
                ret['external_sources'].append(exinf)

        # Check time to beat stats
        xdata = self._req('/game_time_to_beats', f'fields *; limit 500; where game_id = {ret['id']};')
        if xdata and len(xdata) > 0:
            xdata = xdata[0]
            if 'count' in xdata and xdata['count'] > 0:
                ret['time_count'] = xdata['count']
            if 'normally' in xdata and xdata['normally'] > 0:
                ret['time_normal'] = seconds_to_hours(xdata['normally'])
            if 'hastily' in xdata and xdata['hastily'] > 0:
                ret['time_minimal'] = seconds_to_hours(xdata['hastily'])
            if 'completely' in xdata and xdata['completely'] > 0:
                ret['time_full'] = seconds_to_hours(xdata['completely'])
        return ret

    def _parse_company(self, y: dict) -> dict:
        """ Parse company data """
        if 'status' in y:
            if y['status'] in self.support_data['company_status']:
                y['status'] = self.support_data['company_status'][y['status']]
            else:
                Logger.warning(f"Invalid company status: {y['status']}")
        if 'logo' in y:
            if self.company_logos.in_index(y['logo']):
                imginf = self.company_logos.get_row(y['logo'])
                imgpath = self.img_dir + '/' + 'company_' + str(y['slug']) + '.jpg'
                y['logo_url'] = 'https:' + imginf['url'].replace("/t_thumb/", "/t_original/")
                if os.path.exists(imgpath):
                    y['logo'] = imgpath
                else:
                    y['logo'] = download_file(imgpath, y['logo_url'])
            else:
                Logger.warning(f"Company '{y['name']}' logo reference invalid: {y['logo']}")
        if 'start_date' in y and y['start_date'] > 0:
            try:
                dt_object = datetime.datetime.fromtimestamp(y['start_date'])
                if 'start_date_format' in y:
                    y['start_date'] = self._extract_date(dt_object, self.support_data['date_formats'][y['start_date_format']])
                else:
                    y['start_date'] = dt_object.strftime("%Y-%m-%d")
            except Exception as e:
                Logger.warning(f"Company '{y['name']}' established date parsing failed. {e}")
        if 'change_date' in y and y['change_date'] > 0:
            try:
                dt_object = datetime.datetime.fromtimestamp(y['change_date'])
                if 'change_date_format' in y:
                    y['change_date'] = self._extract_date(dt_object, self.support_data['date_formats'][y['change_date_format']])
                else:
                    y['change_date'] = dt_object.strftime("%Y-%m-%d")
            except Exception as e:
                Logger.warning(f"Company '{y['name']}' changed date parsing failed. {e}")
        if 'country' in y:
            if str(y['country']) in self.countries:
                y['country'] = self.countries[str(y['country'])]
            else:
                Logger.warning(f"Unknown country code: {y['country']}")
        y['developed'] = len(y['developed']) if 'developed' in y else 0
        y['published'] = len(y['published']) if 'published' in y else 0
        return y
