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
import dateutil.parser


def extract_html_content(uielem) -> str:
    """
    Extract content from the HTML subtree
    :param uielem: HTML element
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
            ret += extract_html_content(item)
        else:
            ret += item.text.strip()
    return ret


def process_company(data: dict, clist: list):
    """
    Add game companies to the company data table
    :param data: Company data table
    :param clist: List of companies associated with a title
    """
    for x in clist:
        name = x
        if name.endswith(")"):
            sx = name.rfind(" (")
            name = name[0:sx]
        if name in data:
            data[name] += 1
        else:
            data[name] = 1


def print_company_games(data: dict, maxcnt: int=10):
    """
    Print companies by game count
    :param data: Company data table
    :param maxcnt: Number of companies to display
    """
    i = 1
    for k, v in sorted(data.items(), key=lambda x:x[1], reverse=True):
        if i > maxcnt:
            return
        print(f"   {i:2} - {k:30} : {v:3}")
        i += 1


def write_data(data: list, fpath: str, cols: list):
    """
    Write data table to a CSV file
    :param data: Data table
    :param fpath: File path
    :param cols: Data columns
    """
    with open(fpath, 'w', newline='', encoding='utf8') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\r\n', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(cols)

        for row in data:
            writer.writerow(row)
    print(f"Data table saved to {fpath}")

def parse_release_date(rdate: str) -> datetime.datetime:
    """ Parse release date """
    if rdate is None or rdate == 'Unreleased':
        return None

    reldate = None
    tokens = rdate.splitlines()
    for token in tokens:
        if token.endswith(")"):
            sx = token.rfind(" (")
            token = token[0:sx]
        pdate = dateutil.parser.parse(token)
        if reldate is None or pdate < reldate:
            reldate = pdate
    return reldate