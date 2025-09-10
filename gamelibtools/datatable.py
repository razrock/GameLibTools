"""
    Game Library Tools
    Data table

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import csv
import json
import os
from gamelibtools.logger import Logger


class DataTable:
    """ Local data cache / Syncable IGDB backed data table """
    def __init__(self, vkey: str, vname: str, fpath: str, url: str, schema=None, srtc: str = 'id', tsc: str = 'updated_at'):
        """
        Class constructor
        :param vkey: Table key
        :param vname: Table name
        :param fpath: File path (CSV)
        :param url: REST API endpoint (backend URL, used for syncing)
        :param schema: Table schema (columns)
        :param srtc: Sort column name
        :param tsc: Timestamp column/field name
        """
        self.filepath = fpath
        self.tablekey = vkey
        self.data = []
        self.index = {}
        self.schema = schema if schema else []
        self.name = vname
        self.backend = url
        self.tscol = tsc
        self.sortcol = srtc
        self.lastupdate = 0
        self.missingcols = []
        self.issaved = False
        self.syncable = True

    @staticmethod
    def extract_fields(src: dict, params: list) -> dict:
        """ Extract fields from a object (dict) """
        z = {}
        for y in params:
            if y in src:
                z[y] = src[y]
        return z

    def save(self):
        """ Save data table """
        with open(self.filepath, 'w', newline='', encoding='utf8') as csvfile:
            cols = self.get_titles()
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            for row in self.data:
                rdata = self._list_fields(row) if type(row) is dict else row
                writer.writerow(rdata)
        self.issaved = True
        Logger.log(f"Data table stored to {self.filepath}")

    def load(self):
        """ Load data table from a file """
        self.lastupdate = 0
        self.index = {}
        self.missingcols = []
        checkheader = False
        rownum = 0
        with open(self.filepath, 'r', newline='\r\n', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            try:
                for row in reader:
                    if not checkheader:
                        for col in self.schema:
                            cname = (col['title'] if 'title' in col else col['name']) if type(col) is dict else col
                            if cname not in row:
                                self.missingcols.append(col)
                        checkheader = True
                        continue
                    y = self._parse_fields(row)
                    self.add_row(y)
                    rownum += 1
            except Exception as e:
                Logger.error(f"Loading data table {self.filepath} failed, row {rownum}. {e}")
        self.issaved = True
        Logger.log(f"Table '{self.name}' loaded from {self.filepath} - {self.count()} entries")

    def reset(self):
        """ Reset data table """
        self.data = []
        self.index = {}
        self.lastupdate = 0
        self.issaved = False

    def index_rows(self):
        """ Index data table"""
        self.index = {}
        for i in range(len(self.data)):
            idx = self.data[i]['id']
            self.index[idx] = i

    def add_row(self, vrow: dict):
        """ Add / update row data """
        if 'id' not in vrow:
            return
        if self.in_index(vrow['id']):
            self.data[self.index[vrow['id']]] = vrow
        else:
            self.data.append(vrow)
            self.index[vrow['id']] = len(self.data) - 1

        # Update update time
        if self.syncable and self.tscol in vrow and vrow[self.tscol] > self.lastupdate:
            self.lastupdate = vrow[self.tscol]
        self.issaved = False

    def remove_row(self, rid: int):
        """ Remove data row """
        if not self.in_index(rid):
            return
        row = self.data.pop(self.index[rid])
        self.index.pop(rid)
        if self.syncable and self.tscol in row and row[self.tscol] == self.lastupdate:
            self.lastupdate = max(self.data, key=lambda obj: obj[self.tscol])
        self.issaved = False

    def get_row(self, rid: int) -> dict|None:
        """ Get data row """
        if self.index and len(self.index) > 0:
            if rid in self.index:
                return self.data[self.index[rid]]
            else:
                return None
        else:
            return next((item for item in self.data if item["id"] == rid), None)

    def has_file(self) -> bool:
        """ Check if data table file exists """
        return os.path.exists(self.filepath)

    def in_index(self, rid: int) -> bool:
        """ Check if selected entry exists in the index """
        return rid in self.index and self.index[rid] < len(self.data)

    def get_autorefs(self) -> list:
        """ List self-referencing columns in the data table """
        ret = []
        for cx in self.schema:
            if type(cx) is dict and 'ref' in cx and cx['ref'] == self.tablekey:
                ret.append(cx)
        return ret

    def count(self) -> int:
        """ Get row count """
        return len(self.data)

    def update_timestamp(self):
        """ Update table timestamp """
        self.lastupdate = 0
        if not self.syncable:
            return
        for row in self.data:
            if self.tscol in row and row[self.tscol] > self.lastupdate:
                self.lastupdate = row[self.tscol]

    def get_fields(self) -> str:
        """ Extract schema fields """
        ret = ''
        tsinc = False
        hasts = self.tscol and len(self.tscol) > 0
        for c in self.schema:
            if type(c) is dict and 'calc' in c and len(c['calc']) > 0:
                continue
            ret += '' if len(ret) == 0 else ', '
            if type(c) is str:
                ret += c
            elif type(c) is dict and 'field' in c:
                ret += c['field']
            elif type(c) is dict and 'name' in c:
                ret += c['name']
            else:
                raise Exception(f'Invalid data column definition: {c}')
            if hasts and not tsinc and c == self.tscol:
                tsinc = True
        if self.syncable and hasts and not tsinc:
            ret += ('' if len(ret) == 0 else ', ') + self.tscol
        return ret

    def get_missing_fields(self) -> str:
        """ Extract missing schema fields """
        if len(self.missingcols) == 0:
            return ''
        ret = 'id'
        for c in self.missingcols:
            if type(c) is dict and 'calc' in c and len(c['calc']) > 0:
                continue
            ret += ', '
            if type(c) is str:
                ret += c
            elif type(c) is dict and 'field' in c:
                ret += c['field']
            elif type(c) is dict and 'name' in c:
                ret += c['name']
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return ret

    def get_titles(self) -> list:
        """ Get column titles list """
        cols = []
        for c in self.schema:
            if type(c) is str:
                cols.append(c)
            elif type(c) is dict and 'title' in c:
                cols.append(c['title'])
            elif type(c) is dict and 'name' in c:
                cols.append(c['name'])
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return cols

    def get_full_schema(self) -> list:
        """ Get full column schema """
        cols = []
        tsinc = False
        hasts = self.tscol and len(self.tscol) > 0
        for c in self.schema:
            if type(c) is str:
                cols.append({ 'name': c })
            elif type(c) is dict:
                cols.append(c)
            else:
                raise Exception(f'Invalid data column definition: {c}')
            if hasts and not tsinc and c == self.tscol:
                tsinc = True
        if self.syncable and hasts and not tsinc:
            cols.append({ 'name': self.tscol })
        return cols

    def get_missing_schema(self) -> list:
        """ Get full column schema """
        cols = []
        for c in self.missingcols:
            if type(c) is str:
                cols.append({ 'name': c })
            elif type(c) is dict:
                cols.append(c)
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return cols

    def _parse_fields(self, src: list) -> dict:
        """ Parse data row """
        ret = {}
        ind = 0
        for i in range(len(self.schema)):
            if ind >= len(src):
                break
            if self.schema[i] in self.missingcols:
                continue
            try:
                name = self.schema[i] if type(self.schema[i]) is str else self.schema[i]['name']
                dtyp = ('int' if name == 'id' else 'str') if type(self.schema[i]) is str else (self.schema[i]['type'] if 'type' in self.schema[i] else 'str')
                if dtyp == 'list' or dtyp == 'dict' or dtyp == 'img':
                    ret[name] = json.loads(src[ind]) if src[ind] != "" else None
                elif dtyp == 'int' or dtyp == 'count':
                    ret[name] = int(src[ind]) if src[ind] != "" else None
                elif dtyp == 'float':
                    ret[name] = float(src[ind]) if src[ind] != "" else None
                elif dtyp == 'bool':
                    ret[name] = int(src[ind]) != 0 if src[ind] != "" else None
                else:
                    ret[name] = src[ind]
                ind += 1
            except Exception as e:
                raise Exception(f"Parsing data column {i}: {self.schema[i]} failed - {src[ind]}. {e}")
        return ret

    def _list_fields(self, src: dict) -> list:
        """ List data fields in a data row """
        z = []
        for y in self.schema:
            name = y if type(y) is str else y['name']
            dtyp = ('int' if name == 'id' else 'str') if type(y) is str else (y['type'] if 'type' in y else 'str')
            if name in src and src[name] is not None:
                if dtyp == 'list' or dtyp == 'dict' or dtyp == 'img':
                    z.append(json.dumps(src[name]))
                elif dtyp == 'int' or dtyp == 'count':
                    z.append(int(src[name]))
                elif dtyp == 'float':
                    z.append(float(src[name]))
                elif dtyp == 'bool':
                    z.append(1 if src[name] else 0)
                else:
                    z.append(src[name])
            else:
                z.append(None)
        return z