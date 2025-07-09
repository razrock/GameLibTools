"""
    Game Library Tools
    Helper methods

    :author: Milos Jovanovic <milos@tehnocad.rs>
    :copyright: Copyright 2025 by Milos Jovanovic
    :license: This software is licensed under the MIT license
    :license: See LICENSE.txt for full license information
"""
from zipfile import ZipFile

from gamelibtools.logger import Logger


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