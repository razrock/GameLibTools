"""
    Game Library Tools
    Data set

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import datetime
import glob

from gamelibtools.datatable import *
from gamelibtools.igdbclient import IgdbClient
from gamelibtools.logger import Logger
from gamelibtools.util import download_file


class DataSet:
    """ IGDB REST API client """

    def __init__(self, apiclient: IgdbClient, datapath: str, cfgpath: str = './config'):
        """
        Class constructor
        :param datapath: Data directory path
        :param cfgpath: Configuration file path
        """
        self.cfg_dir = cfgpath
        self.data_dir = datapath
        self.tables_dir = self.data_dir + '/tables'
        self.img_dir = self.data_dir + '/images'
        self.datatables = {}
        self.countries = {}
        self.sources = {}
        self.isloaded = False
        self.bigtablesize = 100000
        self.chunksize = 50000
        self.igdbapi = apiclient if apiclient is not None else IgdbClient()
        self._init()

    def load(self):
        """ Load all data tables in the dataset """
        Logger.sysmsg(f"Loading IGDB data tables...")
        for dtkey in self.datatables:
            self.load_table(self.datatables[dtkey])
        self.isloaded = True

    def sync(self):
        """ Sync all data talbes in the dataset """
        if not self.isloaded:
            self.load()

        # Sync tables
        Logger.sysmsg(f"Syncing IGDB data tables...")
        for dtkey in self.datatables:
            if self.datatables[dtkey].syncable:
                dt = self.datatables[dtkey]
                self.sync_table(dt, self._proc_row)

        self.save()

    def save(self):
        """ Save changes """
        for dtkey in self.datatables:
            if not self.datatables[dtkey].issaved:
                self.datatables[dtkey].save()

        # Update timestamps
        if 'timestamps' not in self.sources:
            self.sources['timestamps'] = {}
        for dtkey in self.datatables:
            if self.datatables[dtkey].syncable:
                self.sources['timestamps'][dtkey] = self.datatables[dtkey].lastupdate
        with open(self._get_sources_path(), 'w', encoding='utf-8') as f:
            json.dump(self.sources, f, indent=4)
        Logger.log(f"IGDB sources configuration updated with new sync timestamps")

    def load_table(self, dt: DataTable, fproc = None):
        """ Load or fetch cache table """
        if dt.has_file():
            # Lood local data
            dt.load()
            if len(dt.missingcols) > 0:
                self.expand_table(dt, fproc)
                dt.save()
        else:
            fx = fproc if fproc is not None else self._proc_row
            self.import_table(dt, fx)

    def sync_table(self, dt: DataTable, fproc = None):
        """ Sync table data """
        Logger.log(f"Syncing table '{dt.name}'...")
        total = self.igdbapi.count(dt.backend, f"where {dt.tscol} > {dt.lastupdate};")
        if total == 0:
            Logger.dbgmsg(f"No data found")
            return

        lschema = dt.get_full_schema()
        fx = lambda row: dt.add_row(fproc(row, None, lschema, dt.tablekey))
        self._fetch_table(dt, dt.get_fields(), total, fx, f'where {dt.tscol} > {dt.lastupdate}')

    def import_table(self, dt: DataTable, fproc):
        """ Import data table """
        Logger.log(f"Fetching table '{dt.name}' from IGDB...")

        # Count number of rows / entire table
        total = self.igdbapi.count(dt.backend)
        if total == 0:
            Logger.dbgmsg(f"Table is empty")
            return
        remaining = total

        # Check for current chunk
        query = ''
        maxid = 0
        table_ts = 0
        tmpfile = ''
        store_chunks = total >= self.bigtablesize
        if store_chunks:
            chunk_fname_pref = dt.filepath.replace('.csv', '')
            matching_files = glob.glob(f"{chunk_fname_pref}_*_*.tmp")
            if matching_files and len(matching_files) == 1:
                tmpfile = matching_files[0]
                tokens = tmpfile.replace(chunk_fname_pref + '_', '').replace('.tmp', '').split('_')
                if tokens and len(tokens) == 2:
                    maxid = int(tokens[0])
                    table_ts = int(tokens[1])
                    query = f'where id > {maxid}'
                    if table_ts > 0:
                        query += f' & {dt.tscol} <= {table_ts}'
                dt.load(tmpfile)
            elif dt.tscol != 'id':
                # Fetch current table timestamp
                table_ts = self.igdbapi.maxval(dt.backend, dt.tscol)
                if not table_ts:
                    Logger.dbgmsg(f"Table is empty")
                    return
                query = f'where {dt.tscol} <= {table_ts}'

            # Count remaining entries / remaining chunk
            remaining = self.igdbapi.count(dt.backend, f"{query};")
            if remaining == 0:
                Logger.dbgmsg(f"No data found")
                return

            # Recheck counters
            snap_size = self.igdbapi.count(dt.backend, f"where {dt.tscol} < {table_ts};") if table_ts > 0 and len(tmpfile) > 0 else total
            if snap_size != remaining + dt.count():
                Logger.warning(f"Data table '{dt.name}' remaining rows count doesn't match the expected number")
            Logger.log(f"{total} entries found. Importing data..." if total == remaining else f"{remaining} / {total} entries found. Importing data...")
        elif dt.tscol != 'id':
            # Fetch current table timestamp
            table_ts = self.igdbapi.maxval(dt.backend, dt.tscol)
            if not table_ts:
                Logger.dbgmsg(f"Table is empty")
                return
            query = f'where {dt.tscol} <= {table_ts}'

            # Count number of rows / remaining chunk
            remaining = self.igdbapi.count(dt.backend, f"{query};")
            if remaining == 0:
                Logger.dbgmsg(f"No data found")
                return
            Logger.log(f"{total} entries found. Importing data..." if total == remaining else f"{remaining} / {total} entries found. Importing data...")
        else:
            Logger.log(f"{total} entries found. Importing data...")

        # Download data
        lschema = dt.get_full_schema()
        count = 0
        while count < remaining:
            resp = self.igdbapi.req(dt.backend, f'fields {dt.get_fields()}; offset {count}; limit 500; sort {dt.sortcol} asc; {query};')
            for x in resp:
                y = fproc(x, None, lschema, dt.tablekey)
                if x['id'] > maxid:
                    maxid = x['id']
                dt.add_row(y)
            count += len(resp) if resp else 0
            if store_chunks and count > 0 and count % self.chunksize == 0:
                newfile = dt.filepath.replace('.csv', '') + f"_{maxid}_{table_ts}.tmp"
                dt.save(newfile)
                if len(tmpfile) > 0:
                    os.remove(tmpfile)
                tmpfile = newfile
            if remaining > 1000:
                Logger.report_progress("Loading entries", count, remaining)
            if len(resp) == 0:
                break

        # Resolve autoreferences
        self._resolve_autorefs(dt)

        # Save data table1
        dt.save()
        if len(tmpfile) > 0:
            os.remove(tmpfile)

    def expand_table(self, dt: DataTable, fproc, query: str = ''):
        """ Expand data table """
        mfields = dt.get_missing_fields()
        Logger.log(f"Expanding table {dt.name} with columns {mfields}...")

        lschema = dt.get_missing_schema()
        if mfields == 'id':
            # Local cache processing only
            Logger.dbgmsg(f"Processing local data...")
            for row in dt.data:
                fproc(row, row, lschema, dt.tablekey)
        else:
            # Fetch data from the IGDB
            total = self.igdbapi.count(dt.backend, f"{query};")
            if total == 0:
                Logger.dbgmsg(f"No data found")
                return

            fx = lambda xrow: fproc(xrow, dt.get_row(xrow['id']), lschema, dt.tablekey) if dt.get_row(xrow['id']) else None
            self._fetch_table(dt, mfields, total, fx, query)

    def get_table(self, dname: str) -> DataTable | None:
        """ Get data table """
        return self.datatables[dname] if dname in self.datatables else None

    def resolve_ref(self, idx: int|list, tbl: str, prop: str|list|None):
        """ Resolve data table reference """
        if len(tbl) == 0 or (tbl not in self.datatables and tbl != 'countries') or idx is None:
            return None
        if tbl == 'countries':
            if type(idx) is list:
                ret = []
                for x in idx:
                    if str(x) not in self.countries:
                        Logger.warning(f"Invalid country reference: {x}")
                        continue
                    ret.append(self.countries[str(x)])
                return ret
            else:
                if str(idx) not in self.countries:
                    Logger.warning(f"Invalid country reference: {idx}")
                    return None
                return self.countries[str(idx)]
        else:
            if type(idx) is list:
                ret = []
                for x in idx:
                    ret.append(self._fetch_ref(self.datatables[tbl], x, prop))
                return ret
            else:
                return self._fetch_ref(self.datatables[tbl], idx, prop)

    def _init(self):
        """ Initialize data tables """
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.tables_dir):
            os.makedirs(self.tables_dir)
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)

        self.countries = json.load(open(self.cfg_dir + '/countries.json'))
        Logger.log(f"Countries table loaded - {len(self.countries)} entries")

        if not os.path.exists(self._get_sources_path()):
            return
        self.sources = json.load(open(self._get_sources_path()))
        for name, cfg in self.sources['tables'].items():
            vname = cfg['name'] if 'name' in cfg else name
            fname = os.path.normpath(self.tables_dir + '/' + (cfg['file'] if 'file' in cfg else f"igdb_{name}.csv"))
            vurl = cfg['endpoint'] if 'endpoint' in cfg else f'/{name}'
            cansync = cfg['sync'] if 'sync' in cfg else True
            schema = cfg['schema'] if 'schema' in cfg else None
            sortcol = cfg['sortcol'] if 'sortcol' in cfg else 'id'
            tscol = cfg['tscol'] if 'tscol' in cfg else ('updated_at' if cansync else '')
            self.datatables[name] = DataTable(name, vname, fname, vurl, schema, sortcol, tscol)
            self.datatables[name].syncable = cansync
            if 'timestamps' in self.sources and name in self.sources['timestamps']:
                self.datatables[name].lastupdate = self.sources['timestamps'][name]
        Logger.log(f"IGDB sources configuration loaded - {len(self.datatables)} data tables initialized")

    def _get_sources_path(self) -> str:
        """ Get data sources file path """
        return self.cfg_dir + '/igdbsources.json'

    def _proc_row(self, srcrow: dict, dstrow: dict|None, schema: list, tkey: str) -> dict:
        """ Process data table rows """
        ret = {}
        for cx in schema:
            try:
                iscalc = 'calc' in cx and len(cx['calc']) > 0
                srckey = cx['field'] if 'field' in cx else cx['name']
                if srckey not in srcrow and not iscalc:
                    continue
                vx = srcrow[srckey] if not iscalc else None
                if 'type' in cx and (cx['type'] == 'date' or cx['type'] == 'datetime'):
                    if vx > 0:
                        dto = datetime.datetime.fromtimestamp(vx)
                        vx = dto.strftime("%Y-%m-%d") if cx['type'] == 'date' else dto.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        vx = None
                elif 'type' in cx and cx['type'] == 'count':
                    if iscalc and cx['calc'] in srcrow:
                        vx = len(srcrow[cx['calc']])
                    else:
                        vx = len(vx) if type(vx) is list else None
                elif 'ref' in cx and cx['ref'] != tkey:
                    if vx:
                        isimg = 'type' in cx and cx['type'] == 'img'
                        prop = cx['param'] if 'param' in cx else (cx['prop'] if 'prop' in cx else ('url' if isimg else 'name'))
                        vx = self.resolve_ref(vx, cx['ref'], prop)
                        if isimg:
                            download = cx['download'] if 'download' in cx else True
                            fpref = cx['fileprefix'] if 'fileprefix' in cx else "img"
                            ftokenkey = cx['filetoken'] if 'filetoken' in cx else "slug"
                            ftoken = srcrow[ftokenkey] if ftokenkey in srcrow else str(srcrow['id'])
                            vx = self._resolve_img(vx, fpref, ftoken, download)
                    else:
                        vx = None
                elif 'proc' in cx and cx['prop'] and type(vx) is str:
                    vx = vx.replace("_", " ").capitalize()
                if dstrow:
                    dstrow[cx['name']] = vx
                ret[cx['name']] = vx
            except Exception as e:
                Logger.error(f"Row processing failed, column {cx}, data: {srcrow}. {e}")
        return ret

    def _resolve_autorefs(self, dt: DataTable):
        """ Resolve self references """
        arcols = dt.get_autorefs()
        if len(arcols) == 0:
            return

        # Resolve auto references
        for row in dt.data:
            for col in arcols:
                cname = col['name']
                if cname not in row:
                    continue
                prop = col['param'] if 'param' in col else (col['prop'] if 'prop' in col else 'name')
                row[cname] = self.resolve_ref(row[cname], col['ref'], prop)

    def _resolve_img(self, url: str|list, pref: str, iname: str, download: bool) -> dict|list:
        """ Resolve image reference """
        if type(url) is list:
            ret = []
            for ux in url:
                ret.append(self._fetch_img(ux, pref, iname, download))
            return ret
        else:
            return self._fetch_img(url, pref, iname, download)

    def _fetch_table(self, dt: DataTable, fields: str, total: int, fproc, query: str):
        """ Fetch table data """
        Logger.log(f"{total} entries found. Importing data...")
        count = 0
        while count < total:
            resp = self.igdbapi.req(dt.backend, f'fields {fields}; offset {count}; limit 500; sort {dt.sortcol} asc; {query};')
            for x in resp:
                fproc(x)
            count += len(resp) if resp else 0
            if total > 1000:
                Logger.report_progress("Loading entries", count, total)

    def _fetch_ref(self, dt:DataTable, idx: int, prop: str | list | None):
        """ Fetch reference value """
        if not dt.in_index(idx):
            resp = self.igdbapi.req(dt.backend, f'fields {dt.get_fields()}; limit 500; where id = {idx};')
            if resp is None or len(resp) == 0:
                Logger.warning(f"Invalid '{dt.name}' table reference: {idx}")
                return None
            src = self._proc_row(resp[0], None, dt.get_full_schema(), dt.tablekey)
            dt.add_row(src)
        else:
            src = dt.get_row(idx)

        if type(prop) is list:
            ret = {}
            for key in prop:
                ret[key] = src[key]
            return ret
        else:
            haskey = prop is not None and len(prop) > 0
            return src[prop] if haskey and prop in src else src

    def _fetch_img(self, url: str, pref: str, iname: str, download: bool) -> dict|None:
        """ Fetch image """
        if url is None:
            return None
        ret = {}
        ret['path'] = f"{self.img_dir}/{pref}_{iname}.jpg"
        ret['url'] = 'https:' + url.replace("/t_thumb/", "/t_original/")
        if download and not os.path.exists(ret['path']):
            download_file(ret['path'], ret['url'])
        return ret
