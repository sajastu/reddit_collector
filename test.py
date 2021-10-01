import bz2
import glob
import lzma
import os
import shutil
import zipfile
from functools import partial
from multiprocessing import cpu_count, Pool
from pathlib import Path
import tempfile
import tarfile
import traceback

import zstandard
from bs4 import BeautifulSoup

import urllib.request
import datetime
import requests
import bz2file
import pathlib

from bz2 import decompress

from tqdm import tqdm

from utils import _get_zero_files


def validate_date(date_text, raise_error=False):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m')
        return True
    except ValueError:
        if raise_error:
            raise ValueError("Incorrect data format, should be YYYY-MM")
        else:
            return False


def uncompress_file(file_path):

    def extract_zst(archive):
        input_file = pathlib.Path(archive)
        with open(input_file, 'rb') as compressed:
            output_path = pathlib.Path(store_dir.replace('tmp', 'uncompressed14')) / input_file.stem
            with open(output_path, 'wb') as of:
                decompressor = zstandard.ZstdDecompressor()
                decompressor.copy_stream(compressed, of)

    def extract_bz(archive):
        with bz2.BZ2File(archive) as fr, open(archive.replace('.bz2', '').replace('tmp', 'uncompressed14'), "wb") as fw:
            shutil.copyfileobj(fr, fw)

    def extract_xz(archive):
        fr = lzma.open(archive).read()
        with open(archive.replace('.xz', '').replace('tmp', 'uncompressed14'), "wb") as fw:
            fw.write(fr)

    store_dir = '/'.join(file_path.split('/')[:-1]).replace('tmp', 'uncompressed14')

    try:
        if '.zst' in file_path:
            extract_zst(file_path)
        if '.bz2' in file_path:
            extract_bz(file_path)
        if '.xz' in file_path:
            extract_xz(file_path)
    except Exception as e:
        with open('corrupted-uncompress14.txt', mode='a') as F:
            F.write(f'{file_path} with exception {str(e)}')
            F.write('\n')
            # os.remove(file_path)


def download_zip(url, filePath):
    try:
        def dl_file(url):
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
                with open('corrupted-download.txt', mode='a') as F:
                    F.write(url)
                    F.write('\n')
                print("ERROR, something went wrong")

        file_name = url.split("/")[-1]
        stored_file = filePath + file_name
        # print(f"Downloading started for {url} and will be saved to {stored_file}")
        dl_file(url)
        print(" Downloaded {} ".format(url))
        return stored_file

    except Exception as e:
        with open('corrupted-download.txt', mode='a') as F:
            F.write(url)
            F.write('\n')
        print("ERROR, something went wrong")
        print(e)


class RedditDownloader():
    def __init__(self, store_dir, start_date='2000-01', end_date='2099-12'):
        """

        Base class for downloading and storing bulk files from reddit submissions

        Args:

            store_dir: <str>
                directory where downloaded files are written; absolute path

            start_date: <str>
                downloading bulk files after a given date (default: 2000-01)
                should be in form YYYY-MM

            end_date:
                download bulk files till a given date (default: 2099-12)
                should be in form YYYY-MM

        """

        super(RedditDownloader, self).__init__()

        self._fetched_links = []

        # bulk files should be downloaded from the reddit submissions
        self._homepage = "https://files.pushshift.io/reddit/submissions/"

        # directory to be written
        self._store_dir = store_dir

        # start & end date;
        self._start_date = start_date
        self._end_date = end_date

    def fetch_links(self):

        url = urllib.request.urlopen(self._homepage)
        content = url.read()

        soup = BeautifulSoup(content, 'lxml')

        all_files = soup.findAll('tr', attrs={"class": "file"})
        _files_with_dates = []

        for file in all_files:
            rel_link = file.find('td').find('a')['href']
            date = rel_link.split('.')[-2].split('_')[-1]
            if validate_date(date):
                abs_link = self._homepage + '.'.join(rel_link.split('.')[-2:])

                _files_with_dates.append((abs_link, date))

        # preprocess the links to only preserve correct ones
        # first check to see if user has specified the dates!
        if '2000' not in self._start_date or '2099' not in self._end_date:
            s_year = self._start_date.split('-')[0]
            s_month = self._start_date.split('-')[1]

            e_year = self._end_date.split('-')[0]
            e_month = self._end_date.split('-')[1]

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
                    if should_save: self._fetched_links.append(f[0])
                    should_save = False
        else:
            for f in _files_with_dates:
                self._fetched_links.append(f[0])

    def mp_download_store(self):
        print("There are {} CPUs on this machine ".format(cpu_count()))
        pool = Pool(cpu_count())

        # print(f'Starting to download {len(self._fetched_links)} files')
        # download_func = partial(download_zip, filePath=self._store_dir)
        # for d in tqdm(pool.imap(download_func, self._fetched_links), total=len(self._fetched_links)):
        #     # stored_files.append(d)
        #     pass

        stored_files = []
        for f in glob.glob(self._store_dir + '/' + '*.bz2'):
            stored_files.append(f)

        for f in glob.glob(self._store_dir + '/' + '*.zst'):
            stored_files.append(f)

        for f in glob.glob(self._store_dir + '/' + '*.xz'):
            stored_files.append(f)

        stored_files = sorted(stored_files)
        #
        # stored_files = stored_files[:len(stored_files)//2]
        stored_files = stored_files[(len(stored_files)//2):]
        #
        print(f'Starting to extract {len(stored_files)} files at directory {self._store_dir}')

        # for f in stored_files:
        #     uncompress_file(f)

        for _ in tqdm(pool.imap(uncompress_file, stored_files), total=len(stored_files)):
            pass

        # pool.close()
        # pool.join()
        print('Ended!')

    def _run(self):
        # self.fetch_links()

        # stored_files = []
        # for f in os.listdir(self._store_dir):
        #     stored_files.append(f.split('.')[0])

        # for f in os.listdir(self._store_dir):
        #     if '.' in f:
        #         stored_files.append(f.split('.')[0])
        #
        # zero_ids = _get_zero_files(self._store_dir)
        # updated_links = []
        # for url in self._fetched_links:
        #     if url.split('/')[-1].split('.')[0] not in stored_files:
        # #     if url.split('/')[-1].split('.')[0] in zero_ids:
        #         updated_links.append(url)
        # # self._fetched_links = updated_links[1:]
        # with open('missing_files.txt', mode='w') as fw:
        #     for u in updated_links:
        #         fw.write(u)
        #         fw.write('\n')
        # download corrpupted
        # with  open('corrupted.txt') as F:
        #     for l in F:
        #         updated_links.append(l.strip())
        #
        # self._fetched_links = updated_links[2:]



        # self._fetched_links = updated_links

        # stored_files = []
        # updated_links = []

        # for f in os.listdir(self._store_dir):
        #     if '.' in f:
        #         stored_files.append(f.split('.')[0])

        self.mp_download_store()


if __name__ == '__main__':
    rd = RedditDownloader('/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tmp/')
    rd._run()
