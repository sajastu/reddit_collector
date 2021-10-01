






import argparse
import glob
import os
import shutil
import sys

from bs4 import BeautifulSoup

import urllib.request
import datetime
import requests
import pathlib

from tqdm import tqdm



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


def uncompress_file(file_path):

    def extract_zst(archive):
        input_file = pathlib.Path(archive)
        with open(input_file, 'rb') as compressed:
            output_path = pathlib.Path(store_dir) / input_file.stem
            try:
                with open(output_path, 'wb') as of:
                    decompressor = zstandard.ZstdDecompressor()
                    decompressor.copy_stream(compressed, of)
                print(f'{input_file} is unextracted...')
            except Exception:
                print(f'{input_file} is NOT extracted...')
                print(traceback.format_exc())
                os.remove(output_path)

    def extract_bz(archive):
        try:
            with bz2.BZ2File(archive) as fr, open(archive.replace('.bz2', ''), "wb") as fw:
                shutil.copyfileobj(fr, fw)
        except:
            os.remove(archive.replace('.bz2', ''))

    def extract_xz(archive):
        # fr = lzma.open(archive).read()
        # with open(archive.replace('.xz', ''), "wb") as fw:
        #     import pdb;pdb.set_trace()
        try:
            fr = lzma.open(archive).read()
            with open(archive.replace('.xz', ''), "wb") as fw:
                fw.write(fr)
        except:
            os.remove(archive.replace('.xz', ''))

    store_dir = '/'.join(file_path.split('/')[:-1]).replace('comments', 'comments-uncompressed')

    try:
        if '.zst' in file_path:
            extract_zst(file_path)
        if '.bz2' in file_path:
            extract_bz(file_path)
        if '.xz' in file_path:
            extract_xz(file_path)

        print(f'{file_path} is uncompressed successfully.')
        with open('uncompressed_files.txt', mode='a') as fW:
            fW.write(file_path.split('/')[-1])
            fW.write('\n')

        # os.remove(file_path)

    except:
        with open('corrupted-uncompressed-comments.txt', mode='a') as F:
            F.write(file_path)
            F.write('\n')
            # os.remove(file_path)


def download_zip(url, filePath):
    try:
        def dl_zip(url):
            response = requests.get(url, stream=True)
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1024
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
            with open(filePath + file_name, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)
            progress_bar.close()
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                with open('corrupted-download-comments.txt', mode='a') as F:
                    F.write(url)
                    F.write('\n')
                print("ERROR, something went wrong")

        file_name = url.split("/")[-1]
        stored_file = filePath + file_name
        # print(f"Downloading started for {url} and will be saved to {stored_file}")
        dl_zip(url)
        print(" Downloaded {} ".format(url))
        return stored_file
        # uncompress_file(stored_file)
        # print(" Extracted {}".format(filePath + '/' + file_name))
        # os.remove(stored_file)

    except Exception as e:
        with open('corrupted-download.txt', mode='a') as F:
            F.write(url)
            F.write('\n')
        print("ERROR, something went wrong")
        print(e)


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


def validate_link(link):
    if 'RC_2020-01.zst' in link or 'RC_2020-02.zst' in link or 'RC_2020-03.zst' in link \
    or 'RC_2020-04.zst' in link or 'RC_2020-05.zst' in link or 'RC_2020-06.zst' in link \
    or 'RC_2020-07.zst' in link or 'RC_2020-08.zst' in link or 'RC_2020-09.zst' in link \
    or 'RC_2020-10.zst' in link or 'RC_2020-11.zst' in link or 'RC_2020-12.zst' in link:
        return False
    else:
        return True




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-homepage", default='https://files.pushshift.io/reddit/comments/',
                        type=str, help='Submission/Comments home dir')
    parser.add_argument("-start_date", default='2000-01', type=str, help='Date range --start; if not specified, will get through all dates')
    parser.add_argument("-end_date", default='2099-12', type=str, help='Date range --end; if not specified, will get through all dates')

    args = parser.parse_args()

    links = fetch_links(args.homepage, args.start_date, args.end_date)


    ids = []
    ids_link = {}
    for l in links:
        ids.append(l.split('/')[-1].split('.')[0].strip())
        ids_link[l.split('/')[-1].split('.')[0].strip()] = l

    not_ids = {}
    stored_files = []
    for root, dirs, files in os.walk("/mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/", topdown=False):
        for name in files:
            if 'RC' in name and '.' in name:
                if 'missed_comments' not in root:
                    stored_files.append(os.path.join(root, name))

        # print('HWWW')
        # for name in dirs:
        #     print(os.path.join(root, name))
            # print(name)


    for st in stored_files:
        print(st)
