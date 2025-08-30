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
    """ IGDB REST API client """

    def __init__(self, fpath: str = 'data.csv', schema=None):
        """
        Class constructor
        :param fpath: File path (CSV)
        :param schema: Table schema (columns)
        """
        self.filepath = fpath
        self.data = []
        self.index = {}
        self.schema = schema if schema else []

    @staticmethod
    def save_table(rows: list|dict, fpath: str, schema: list):
        """
        Export games data table to a CSV file
        :param rows: Data table
        :param fpath: File path
        :param schema: Data table schema
        """
        with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
            cols = DataTable.get_schema_columns(schema)
            writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(cols)

            if type(rows) is list:
                # Write array
                for row in rows:
                    rdata = DataTable.list_fields(row, schema) if type(row) is dict else row
                    writer.writerow(rdata)
            else:
                # Write map
                for rid, row in rows.items():
                    rdata = DataTable.list_fields(row, schema) if type(row) is dict else row
                    rdata[0] = rid
                    writer.writerow(rdata)
        Logger.log(f"Data table stored to {fpath}")

    @staticmethod
    def load_table(fpath: str, schema: list) -> list:
        """
        Load data table
        :param fpath: File path
        :param schema: Data table schema
        :return: Data table rows
        """
        ret = []
        checkheader = False
        rownum = 0
        with open(fpath, 'r', newline='\r\n', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            try:
                for row in reader:
                    if not checkheader:
                        checkheader = True
                        continue
                    y = DataTable.parse_fields(row, schema)
                    ret.append(y)
                    rownum += 1
            except Exception as e:
                Logger.error(f"Parsing data table {fpath} failed, row {rownum}. {e}")
        return ret

    @staticmethod
    def index_table(rows: list, key: str = 'id') -> dict:
        """ Index data table"""
        ret = {}
        for i in range(len(rows)):
            idx = rows[i][key]
            ret[idx] = i
        return ret

    def save(self):
        """ Save data table to a CSV file """
        DataTable.save_table(self.data, self.filepath, self.schema)

    def load(self, index: bool = True):
        """ Load data table """
        self.data = DataTable.load_table(self.filepath, self.schema)
        if index:
            self.index_rows()

    def reset(self):
        """ Reset data table """
        self.data = []
        self.index = {}

    def index_rows(self, key: str = 'id'):
        """ Index data table"""
        self.index = DataTable.index_table(self.data, key)

    def add_row(self, vrow: dict, index: bool = True):
        """ Add row data """
        self.data.append(vrow)
        if index:
            self.index[vrow['id']] = len(self.data) - 1

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

    def count(self) -> int:
        """ Get row count """
        return len(self.data)

    @staticmethod
    def extract_fields(src: dict, params: list) -> dict:
        """ Extract fields from a object (dict) """
        z = {}
        for y in params:
            if y in src:
                z[y] = src[y]
        return z

    @staticmethod
    def list_fields(src: dict, schema: list) -> list:
        """ List data fields in a data row """
        z = []
        for y in schema:
            name = y if type(y) is str else y['name']
            dtyp = ('int' if name == 'id' else 'str') if type(y) is str else y['type']
            if name in src and src[name] is not None:
                if dtyp == 'list' or dtyp == 'dict':
                    z.append(json.dumps(src[name]))
                elif dtyp == 'int':
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

    @staticmethod
    def parse_fields(src: list, schema: list) -> dict:
        """ Parse data row """
        ret = {}
        for i in range(len(schema)):
            if i >= len(src):
                break
            try:
                name = schema[i] if type(schema[i]) is str else schema[i]['name']
                dtyp = ('int' if name == 'id' else 'str') if type(schema[i]) is str else schema[i]['type']
                if dtyp == 'list' or dtyp == 'dict':
                    ret[name] = json.loads(src[i]) if src[i] != "" else None
                elif dtyp == 'int':
                    ret[name] = int(src[i]) if src[i] != "" else None
                elif dtyp == 'float':
                    ret[name] = float(src[i]) if src[i] != "" else None
                elif dtyp == 'bool':
                    ret[name] = int(src[i]) != 0 if src[i] != "" else None
                else:
                    ret[name] = src[i]
            except Exception as e:
                raise Exception(f"Parsing data column {i}: {schema[i]} failed - {src[i]}")
        return ret

    @staticmethod
    def get_schema_columns(schema: list) -> list:
        """ Parse schema columns """
        ret = []
        for c in schema:
            if type(c) is str:
                ret.append(c)
            elif type(c) is dict and 'name' in c:
                ret.append(c['name'])
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return ret

    @staticmethod
    def get_schema_fields(schema: list) -> str:
        """ Extract schema fields """
        ret = ''
        for c in schema:
            ret += '' if len(ret) == 0 else ', '
            if type(c) is str:
                ret += c
            elif type(c) is dict and 'name' in c:
                ret += c['name']
            else:
                raise Exception(f'Invalid data column definition: {c}')
        return ret