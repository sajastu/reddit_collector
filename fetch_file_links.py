import argparse
import datetime
import os
import pathlib
import shutil
import urllib.request

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_links(homepage, start_date='2015-06', end_date='2099-12'):

    url = urllib.request.urlopen(homepage)
    content = url.read()

    soup = BeautifulSoup(content, 'lxml')

    all_files = soup.findAll('tr', attrs={"class": "file"})
    _files_with_dates = []

    for file in all_files:
        rel_link = file.find('td').find('a')['href']
        date = rel_link.split('.')[-2].split('_')[-1]
        if validate_date(date):
            abs_link = homepage + '.'.join(rel_link.split('.')[-2:]).replace('/', '')

            _files_with_dates.append((abs_link, date))

    # preprocess the links to only preserve correct ones
    # first check to see if user has specified the dates!
    fetched_links = []
    if '2000' not in start_date or '2099' not in end_date:
        s_year = start_date.split('-')[0]
        s_month = start_date.split('-')[1]

        e_year = end_date.split('-')[0]
        e_month = end_date.split('-')[1]

        should_save = False
        for f in _files_with_dates:
            file_date = f[-1]
            f_year = file_date.split('-')[0]
            f_month = file_date.split('-')[1]

            if f_year >= s_year and f_year <= e_year:
                if f_year == s_year:
                    if f_month >= s_month:
                        should_save = True
                elif f_year == e_year:
                    if f_month <= e_month:
                        should_save = True
                else:
                    should_save = True
                if should_save: fetched_links.append(f[0])
                should_save = False
    else:
        for f in _files_with_dates:
            fetched_links.append(f[0])

    return fetched_links


def validate_date(date_text, raise_error=False):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m')
        return True
    except ValueError:

        try:
            datetime.datetime.strptime(date_text, '%Y-%m-%d')
            return True

        except:

            if raise_error:
                raise ValueError("Incorrect data format, should be YYYY-MM")
            else:
                return False




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-scrap", default='comments', type=str, help='Submission/Comments home dir')
    parser.add_argument("-start_date", default='2000-01', type=str, help='Date range --start; if not specified, will get through all dates')
    parser.add_argument("-end_date", default='2099-12', type=str, help='Date range --end; if not specified, will get through all dates')

    args = parser.parse_args()
    args.homepage = f'https://files.pushshift.io/reddit/{args.scrap}/'
    links = fetch_links(args.homepage, args.start_date, args.end_date)

    ids = []
    ids_link = {}
    for l in links:
        ids.append(l.split('/')[-1].split('.')[0].strip())
        ids_link[l.split('/')[-1].split('.')[0].strip()] = l

    for k, v in ids_link.items():
        # if validate_link(v):
        print(v)