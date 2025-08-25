"""
    Game Library Tools
    Helper methods

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
import csv
import datetime
import json
import requests
from zipfile import ZipFile
from gamelibtools.logger import Logger
from io import BytesIO


def extract_html_content(uielem, splitarr: bool=True) -> str:
    """
    Extract content from the HTML subtree
    :param uielem: HTML element
    :param splitarr: Split elements into array (text lines)
    :return: Element content (text)
    """
    if uielem is None:
        return ""
    ret = ""
    for item in uielem.contents:
        if item.name == "br":
            ret += "\r\n"
        elif item.name == "sup":
            ret += " (" + item.text.strip() + ")"
        elif item.name == "small":
            ret += " " + item.text.strip()
        elif item.name == "ul" or item.name == "ol":
            skip = True
            for x in item.contents:
                ret += ("" if skip else "\r\n") + x.text.strip()
                skip = False
        elif item.name == "div":
            ret += extract_html_content(item, splitarr)
        elif item.name == "p":
            ret += extract_html_content(item, splitarr)
        else:
            tx = item.text.strip()
            if splitarr:
                if tx.lower() == "and" or tx.lower() == "," or tx == "/" or tx == "•":
                    continue
                ret += ("" if len(ret) == 0 else "\r\n") + tx
            else:
                ret += tx
    return ret.strip()

def process_stat_list(data: dict, clist: list):
    """
    Add tag/company/category to the stat count
    :param data: Stat count table
    :param clist: List of items with a title
    """
    if clist is None:
        return
    for x in clist:
        if x == '':
            continue
        name = x
        if name.endswith(")"):
            sx = name.rfind(" (")
            name = name[0:sx]
        if name in data:
            data[name] += 1
        else:
            data[name] = 1

def print_stat(data: dict, maxcnt: int=10):
    """
    Print game count statistics
    :param data: Company data table
    :param maxcnt: Number of companies to display
    """
    i = 1
    for k, v in sorted(data.items(), key=lambda x:x[1], reverse=True):
        if i > maxcnt:
            return
        Logger.log(f"   {i:2} - {k:30} : {v:3}")
        i += 1

def print_array(arr: list):
    """
    Print array
    :param arr: Data array
    :return: Array text representation
    """
    if arr is None:
        return None
    if len(arr) == 0:
        return ''
    return str(arr)

def get_zip_uncompressed_size(fpath: str) -> int:
    """
    Calculates the total uncompressed size of all files within a ZIP archive.
    :param fpath: File path
    :return: Total uncompressed size in bytes
    """
    ret = 0
    try:
        with ZipFile(fpath, 'r') as zip_file:
            for member_name in zip_file.namelist():
                info = zip_file.getinfo(member_name)
                ret += info.file_size
    except FileNotFoundError:
        Logger.error(f"Error: ZIP file not found at {fpath}")
    except Exception as e:
        Logger.error(f"An error occurred: {e}")
    return ret

def download_file(fpath: str, url: str) -> str|None:
    """ Download file """
    Logger.dbgmsg(f"Downloading file {fpath} from {url}...")
    try:
        response = requests.get(url)
        if response is None:
            return None

        imgbytes = BytesIO(response.content)
        with open(fpath, 'wb') as f:
            f.write(imgbytes.getbuffer())
        return fpath
    except Exception as e:
        Logger.error(f"Downloading file {fpath} from {url} failed. {e}")
        return None

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

def extract_fields(src: dict, params: list) -> dict:
    """ Extract fields from a object (dict) """
    z = {}
    for y in params:
        if y in src:
            z[y] = src[y]
    return z

def list_fields(src: dict, schema: list) -> list:
    """ List data fields in a data row """
    z = []
    for y in schema:
        name = y if type(y) is str else y['name']
        dtyp = ('int' if name == 'id' else 'str') if type(y) is str else y['type']
        if name in src and src[name]:
            if dtyp == 'list' or dtyp == 'dict':
                z.append(json.dumps(src[name]))
            elif dtyp == 'int':
                z.append(int(src[name]))
            elif dtyp == 'float':
                z.append(float(src[name]))
            else:
                z.append(src[name])
        else:
            z.append(None)
    return z

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
            else:
                ret[name] = src[i]
        except Exception as e:
            raise Exception(f"Parsing data column {i}: {schema[i]} failed - {src[i]}")
    return ret

def save_data_table(rows: list, fpath: str, schema: list):
    """
    Export games data table to a CSV file
    :param rows: Data table
    :param fpath: File path
    :param schema: Data table schema
    """
    with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
        cols = get_schema_columns(schema)
        writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(cols)

        for row in rows:
            rdata = list_fields(row, schema)
            writer.writerow(rdata)
    Logger.log(f"Data table stored to {fpath}")

def load_data_table(fpath: str, schema: list) -> list:
    """ Load data table """
    ret = []
    checkheader = False
    rownum = 0
    cols = get_schema_columns(schema)
    with open(fpath, 'r', newline='\r\n', encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        try:
            for row in reader:
                if not checkheader:
                    if len(row) != len(schema):
                        Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch")
                        return []
                    for i in range(len(row)):
                        if row[i] != cols[i]:
                            Logger.error(f"Loading table data from {fpath} failed. Column schema missmatch (column {i})")
                            return []
                    checkheader = True
                    continue
                y = parse_fields(row, schema)
                ret.append(y)
                rownum += 1
        except Exception as e:
            Logger.error(f"Parsing data table {fpath} failed, row {rownum}. {e}")
    return ret

def index_data_table(dtable: list, key: str = 'id') -> dict:
    """ Index data table"""
    ret = {}
    for i in range(len(dtable)):
        idx = dtable[i][key]
        ret[idx] = i
    return ret

def seconds_to_hours(vsec: int) -> float:
    """ Convert number of seconds to number of hours """
    vhours = vsec / 3600.0
    vhoursi = int(vhours)
    diffm = vhours - float(vhoursi)
    return float(vhoursi) if diffm < 0.33 else (float(vhoursi + 1) if diffm >= 0.67 else float(vhoursi) + 0.5)