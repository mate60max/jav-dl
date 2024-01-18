#!/usr/bin/env python3

import os
import re
import sys
import json
from threading import local
import time
import datetime
import requests
import json
import queue
import logging
from tqdm import tqdm
import shutil
import signal
import concurrent.futures
from selenium import webdriver as wd

USE_PROXY = False

logging.basicConfig(level=logging.INFO)
parser_logger = logging.getLogger("myparser")
parser_logger.setLevel(logging.INFO)
http_logger = logging.getLogger("myhttp")
http_logger.setLevel(logging.INFO)

PROXIES = dict(http='http://127.0.0.1:7890',
               https='http://127.0.0.1:7890') if USE_PROXY else {}

CHROME_OPTIONS = wd.ChromeOptions()
# CHROME_OPTIONS.add_argument('headless')
# CHROME_OPTIONS.add_argument('no-startup-window')
CHROME_OPTIONS.add_argument('incognito')
# CHROME_OPTIONS.add_argument('--start-minimized')
CHROME_OPTIONS.add_argument('blink-settings=imagesEnabled=false') 
CHROME = None


def restart_chrome():
    global CHROME
    if CHROME:
        CHROME.quit()
        time.sleep(1)
    CHROME = wd.Chrome(options=CHROME_OPTIONS)

def close_chrome():
    global CHROME
    if CHROME:
        CHROME.quit()
        time.sleep(1)
        CHROME = None

def load_fake_page():
    with open('page.txt', 'r') as f:
        return f.read()

def save_page(page):
    with open('page.txt', 'w') as f:
        f.write(page)
        f.flush()

def http_get(url, log_start=None, interval_sec=3, retry=10, sleep_sec=1):
    if log_start:
        logging.info(log_start)
    # return load_fake_page()
    for i in range(retry):
        try:
            http_logger.debug(f'try to get page: {url}')
            if not CHROME:
                restart_chrome()
            CHROME.delete_all_cookies()
            CHROME.get(url)
            time.sleep((interval_sec+1)/2)
            res = CHROME.page_source
            # chrome.quit()
            time.sleep((interval_sec+1)/2-1)
            # http_logger.debug(res)
            # save_page(res)
            return res
        except BaseException as e:
            http_logger.debug(e)
            http_logger.warning(
                f'[x] Failed: {url}, retry={i+1}/{retry}')
            time.sleep(sleep_sec+i)
            if i >= retry / 2:
                restart_chrome()
    http_logger.error(f'Failed to get page: {url}')
    return None

def http_download(url, output_path, log_start=None, remove_if_err=False, show_progress=False, retry=10, sleep_sec=1):
    if log_start:
        logging.info(log_start)
    for i in range(retry):
        try:
            http_logger.debug(f'try to download: {url}')
            res = requests.get(url, stream=True, allow_redirects=True, timeout=10,
                               proxies=PROXIES, verify=True)  # 'xchina-co-chain.pem')
            total_size_in_bytes = int(
                res.headers.get('content-length', 0))
            if not show_progress:
                with open(output_path, 'wb') as f:
                    for chunk in res.iter_content(chunk_size=8192):
                        f.write(chunk)
            else:
                with open(output_path, "wb") as f:
                    with tqdm.wrapattr(res.raw, "read", total=total_size_in_bytes, desc="Downloading", colour='green') as r_raw:
                        shutil.copyfileobj(r_raw, f)
                http_logger.info(f'Downloaded: {os.path.getsize(output_path)} bytes, total: {total_size_in_bytes} bytes')
            if os.path.exists(output_path):
                if os.path.getsize(output_path) == total_size_in_bytes:
                    return True
        except BaseException as e:
            http_logger.warning(
                f'[x] Failed: {url}, retry={i+1}/{retry}')
            time.sleep(sleep_sec+i)
    http_logger.error(f'Failed to download: {url}')
    if remove_if_err:
        if os.path.exists(output_path):
            os.remove(output_path)
    return False

def read_plain_urls(path):
    with open(path, 'r') as f:
        return f.readlines()

def write_plain_urls(urls, file):
    with open(file, 'w') as f:
        for url in urls:
            f.write(f'{url}\n')

default_executor = None
def signal_handler(sig, frame):
    logging.warning('You pressed Ctrl+C!')
    if default_executor:
        logging.info(f'Tasks remaining in executor.queue: {default_executor._work_queue.qsize()} to cancel.')
        default_executor.shutdown(False, cancel_futures=True)
        logging.warning(f'The default_executor has been shutdown..')
    # do something here

# deprecated
def parse_vol_series(vol):
    pats = re.findall(r'^(\d+)[-_\.]{1}(\d+)$', vol)
    if len(pats) == 1:
        return pats[0][0][-2:] if len(pats[0][0]) > 2 else pats[0][0]
    pats = re.findall(r'^(\d+)-([a-zA-Z]+)$', vol)
    if len(pats) == 1:
        return pats[0][0][:2] if len(pats[0][0]) > 2 else pats[0][0]

    if vol.find('.') > 0:
        return vol.split('.')[0]
    elif vol.find('_') > 0:
        return vol.split('_')[0]
    elif vol.find('-') > 0:
        return vol.split('-')[0]

    pats = re.findall(r'^(\d*[a-zA-Z]+)(\d+)$', vol)
    if len(pats) == 1:
        return pats[0][0]
    
    return vol

class UrlParser:

    DEFAULT_URL_ROOT = 'https://javdb.com'

    _actor_id_pattern = re.compile(r'/actors/(\w+)')
    _movie_id_pattern = re.compile(r'/v/(\w+)')
    _url_root_pattern = re.compile(r'(https://[^/]+)/')
    _url_path_pattern = re.compile(r'(https://[^\?]+)')

    @staticmethod
    def parse_actor_id(url):
        pats = re.findall(UrlParser._actor_id_pattern, url)
        parser_logger.debug(pats)
        return pats[0] if len(pats) > 0 else None

    @staticmethod
    def parse_movie_id(url):
        pats = re.findall(UrlParser._movie_id_pattern, url)
        parser_logger.debug(pats)
        return pats[0] if len(pats) > 0 else None

    @staticmethod
    def parse_url_root(url):
        pats = re.findall(UrlParser._url_root_pattern, url)
        parser_logger.debug(pats)
        return pats[0] if len(pats) > 0 else None

    @staticmethod
    def parse_url_path(url):
        pats = re.findall(UrlParser._url_path_pattern, url)
        parser_logger.debug(pats)
        return pats[0] if len(pats) > 0 else None
    
    @staticmethod
    def parse_url_file(url):
        return url.split('/')[-1].split('?')[0]
    
    @staticmethod
    def parse_file_ext(file):
        return file.split('.')[-1]
    
    @staticmethod
    def get_full_url(url_root, relative_path):
        if relative_path.startswith('http'):
            return relative_path
        return f'{url_root}{relative_path}' if relative_path.startswith('/') else f'{url_root}/{relative_path}'
    
    @staticmethod
    def get_actor_url(url_root, actor_id):
        return f'{url_root}/actors/{actor_id}'
    
    @staticmethod
    def get_movie_url(url_root, movie_id):
        return f'{url_root}/v/{movie_id}'
    
    @staticmethod
    def get_search_url(keyword, url_root=DEFAULT_URL_ROOT):
        return  UrlParser.get_full_url(url_root, f'search?q={keyword}')
    
    @staticmethod
    def is_actor_url(url):
        return UrlParser.parse_actor_id(url) != None

    @staticmethod
    def is_movie_url(url):
        return UrlParser.parse_movie_id(url) != None
    
class ActorParser:

    _title_pattern = re.compile(r'<title>(.*?)</title>')
    _h1_pattern = re.compile(r'<h2 class="title(.*?)</h2>', re.DOTALL)
    _name_pattern = re.compile(r'<span class="actor-section-name">([^<]+)</span>')
    _other_names_pattern = re.compile(r'<span class="section-meta">(.+)</span>')
    _cover_pattern = re.compile(r'<span class="avatar" style="background-image: url\((http[^\)]+)\)"')
    
    _next_page_pattern = re.compile(r'<a rel="next" class="pagination-next" href="([^"]+)">Next</a>')
    
    _movie_item_pattern = re.compile(r'<div class="item(.*?)</a>', re.DOTALL)
    _movie_url_pattern = re.compile(r'<a href="(/v/[\w]+)"', re.DOTALL)
    _movie_cover_pattern = re.compile(r'src="([^"]+)"', re.DOTALL)
    _movie_vol_pattern = re.compile(
        r'<div class="video-title"><strong>([^<]+)</strong>(.*?)</div>', re.DOTALL)
    _movie_date_pattern = re.compile(
        r'([\d]{2})/([\d]{2})/([\d]{4})', re.DOTALL)
    

    @staticmethod
    def parse_actor_desc(page):
        # print(page)
        title_pats = re.findall(ActorParser._title_pattern, page)
        parser_logger.debug(title_pats)
        title = title_pats[0]
        latname = title.split('|')[0].replace(' ', '')
        if not latname.isalpha():
            latname = ''

        h1_pats = re.findall(ActorParser._h1_pattern, page)
        parser_logger.debug(h1_pats)
        h1 = h1_pats[0]
        
        name_pats = re.findall(ActorParser._name_pattern, h1)
        parser_logger.debug(name_pats)
        name = name_pats[0].replace(' ', '') if len(name_pats) > 0 else ''
        othernames = []
        names = name.split(',')
        if len(names) > 1:
            chinesename = names[0]
            name = names[1]
            if len(names) > 2:
                othernames = names[2:]
        else:
            chinesename = name

        other_names_pats = re.findall(ActorParser._other_names_pattern, h1)
        parser_logger.debug(other_names_pats)
        other_names = other_names_pats[0].replace(' ', '') if len(other_names_pats) > 1 else ''
        movie_number = int(other_names_pats[-1].split(' ')[0]) if len(other_names_pats) > 0 else 0
        othernames.extend(other_names.split(','))
        
        cover_pats = re.findall(ActorParser._cover_pattern, page)
        parser_logger.debug(cover_pats)
        cover = cover_pats[0] if len(cover_pats) > 0 else ''

        return {
            'name': name,
            'latname': latname,
            'chnname': chinesename,
            'othnames': othernames,
            'movies': movie_number,
            'avatar': cover
        }

    @staticmethod
    def parse_actor_next(page):
        pats = re.findall(ActorParser._next_page_pattern, page)
        parser_logger.debug(pats)
        return pats[0] if len(pats) > 0 else None

    @staticmethod
    def parse_actor_movies(page):
        movies = {}
        index = page.find('<div class="movie-list')
        if index > 0:
            page = page[index:]
            parser_logger.debug(page)
        else:
            parser_logger.warning('parse_actor_movies failed: no section movies')
            return movies
        
        movie_item_pats = re.findall(ActorParser._movie_item_pattern, page)
        parser_logger.debug(f'find movie items: {len(movie_item_pats)}')
        if len(movie_item_pats) > 0:
            parser_logger.debug(movie_item_pats[0])
            last_date = datetime.datetime.now().strftime("%Y-%m-%d")
            for pat in movie_item_pats:
                movie_url_pats = re.findall(ActorParser._movie_url_pattern, pat)
                parser_logger.debug(movie_url_pats)
                if len(movie_url_pats) <= 0:
                    parser_logger.error(f'movie_url_pats not found: {pat}')
                    continue
                cover_pats = re.findall(ActorParser._movie_cover_pattern, pat)
                parser_logger.debug(cover_pats)
                vol_pats = re.findall(ActorParser._movie_vol_pattern, pat)
                parser_logger.debug(vol_pats)
                date_pats = re.findall(ActorParser._movie_date_pattern, pat)
                parser_logger.debug(date_pats)
                parser_logger.debug(f'len of (url,cover,vol,date) = ({len(movie_url_pats)}, {len(cover_pats)}, {len(vol_pats)}, {len(date_pats)})')

                movie_id = UrlParser.parse_movie_id(movie_url_pats[0])
                if not movie_id:
                    parser_logger.error(f'movie_id not found: {movie_url_pats[0]}')
                    continue
                if len(date_pats) > 0:
                    date_f = f'{date_pats[0][2]}-{date_pats[0][0]}-{date_pats[0][1]}'
                    last_date = date_f
                else:
                    date_f = last_date
                movies[movie_id] = {
                    'id': movie_id,
                    'url': movie_url_pats[0],
                    'cover': cover_pats[0].strip() if len(cover_pats) > 0 else '',
                    'vol': vol_pats[0][0].strip() if len(vol_pats) > 0 else '',
                    'title': vol_pats[0][1].strip() if len(vol_pats) > 0 else 'title',
                    'date': date_f
                }
        return movies

class ActorHelper:

    DEFAULT_ACTOR_DB_DIR='javdb/actors'

    @staticmethod
    def save_actor(actor, actor_dir=DEFAULT_ACTOR_DB_DIR):
        if not os.path.exists(actor_dir):
            os.makedirs(actor_dir)
        actor_file = os.path.join(actor_dir, f'{actor["id"]}.json')
        with open(actor_file, 'w') as f:
            json.dump(actor, f, indent=2, ensure_ascii=False)
            f.write('\n')

    @staticmethod
    def load_actor(actor_id, actor_dir=DEFAULT_ACTOR_DB_DIR):
        actor_file = os.path.join(actor_dir, f'{actor_id}.json')
        if not os.path.exists(actor_file):
            return None
        with open(actor_file, 'r') as f:
            actor = json.load(f)
        return actor

    @staticmethod
    def load_actors(actor_dir=DEFAULT_ACTOR_DB_DIR):
        if not os.path.exists(actor_dir):
            os.makedirs(actor_dir)
        actor_files = os.listdir(actor_dir)
        actors = {}
        for actor_file in actor_files:
            if actor_file.startswith('.'):
                continue
            if not actor_file.endswith('.json'):
                continue
            with open(os.path.join(actor_dir, actor_file), 'r') as f:
                actor = json.load(f)
                actors[actor['id']] = actor
        logging.info(f'Loaded {len(actors)} actors')
        return actors

    @staticmethod
    def merge_actor(old_actor, new_actor):
        if old_actor:
            old_actor['movies'].update(new_actor['movies'])
            new_actor['movies'] = old_actor['movies']
        return new_actor

    @staticmethod
    def update_save_actor(actor, actor_dir=DEFAULT_ACTOR_DB_DIR):
        new_actor = ActorHelper.merge_actor(ActorHelper.load_actor(actor['id'], actor_dir), actor)
        ActorHelper.save_actor(new_actor, actor_dir)
        logging.info(f'Updated and saved actor: {actor["id"]}')
        return new_actor

    @staticmethod
    def get_actor_avatar_dir(data_dir, mkdir=False):
        dir = os.path.join(data_dir, 'avatars')
        if mkdir:
            os.makedirs(dir, exist_ok=True)
        return dir

    @staticmethod
    def get_actor_avatar_file(data_dir, actor_id, avatar_file_ext, mkdir=False):
        return os.path.join(ActorHelper.get_actor_avatar_dir(data_dir, mkdir=mkdir), f'{actor_id}.{avatar_file_ext}')

    @staticmethod
    def pull_actor_page(url, useOriginUrl=False, actor=None):
        url_root = UrlParser.parse_url_root(url)
        actor_id = UrlParser.parse_actor_id(url)
        actor_url = url if useOriginUrl else UrlParser.get_actor_url(url_root, actor_id)
        parser_logger.debug(actor_url)
        
        logging.info(f'[A]{actor_id}: Parsing {actor_url}')
        res = http_get(actor_url)
        if not res:
            return None
        page = res
        # print(page)
        summary = ActorParser.parse_actor_desc(page)
        logging.info(f'[A]{actor_id}: {summary["name"]}({summary["chnname"]}), {summary["movies"]} movies')

        old_movies = actor.get('movies', {}) if actor else {}
        logging.info(f'[A]{actor_id}: Old movies got {len(old_movies)}')
        movies = {}
        pages = 1
        all_new = True
        while page and all_new:
            next_url = ActorParser.parse_actor_next(page)
            parsed_movies = ActorParser.parse_actor_movies(page)
            for key in parsed_movies.keys():
                if key in old_movies:
                    all_new = False
                movies[key] = parsed_movies[key]
            logging.info(f'[A]{actor_id}: Parsed {len(parsed_movies)} movies, Page {pages}')
            if not all_new:
                logging.info(f'[A]{actor_id}: All new movies got, break for older ones')
                break
            if next_url:
                res = http_get(UrlParser.get_full_url(url_root, next_url))
                page = res if res else None
                pages = pages + 1
                continue
            page = None
        logging.info(f'[A]{actor_id}: Total got {len(movies)} movies')
        return {
            'id': actor_id,
            'url': actor_url,
            'summary': summary,
            'movies': movies
        }

    @staticmethod
    def print_actor_summary(actor):
        print('Actor summary:')
        print(f' - id: {actor["id"]}')
        print(f' - name: {actor["summary"]["name"]}')
        print(f' - latname: {actor["summary"]["latname"]}')
        print(f' - chinesename: {actor["summary"]["chnname"]}')
        print(f' - othernames: {actor["summary"]["othnames"]}')
        print(f' - movie_number: {actor["summary"]["movies"]}')
        print(f' - movies_listed: {len(actor["movies"])}')

    @staticmethod
    def count_actors():
        actors = ActorHelper.load_actors()
        return len(actors)

class MovieParser:

    _movie_title_pattern = re.compile(r'<strong class="current-title">([^<]+)</strong>')

    _movie_cover_pattern = re.compile(r'<img src="([^"]+)" class="video-cover">')

    _movie_tag_section_pattern = re.compile(r'<strong>Tags:</strong>(.*?)</span>', re.DOTALL)
    _movie_tags_pattern = re.compile(r'<a href="([^"]+)">([^<]+)</a>')

    _movie_actors_section_pattern = re.compile(r'<strong>Actor\(s\):</strong>(.*?)</span>', re.DOTALL)
    _movie_actors_pattern = re.compile(r'<a href="([^"]+)">([^<]+)</a>')

    _movie_vol_section_pattern = re.compile(r'<strong>ID:</strong>(.*?)</span>', re.DOTALL)
    _movie_vol_pattern = re.compile(r'<a href="([^"]+)">([^<]+)</a>.*?(-[\d]+)', re.DOTALL)

    _movie_date_section_pattern = re.compile(r'<strong>Released Date:</strong>(.*?)</span>', re.DOTALL)
    _movie_date_pattern = re.compile(r'([\d]{4}-[\d]{2}-[\d]{2})', re.DOTALL)

    _movie_preview_section_pattern = re.compile(r'<article(.*?)</article>', re.DOTALL)
    _movie_preview_videos_pattern = re.compile(r'<source src="([^"]+)"')
    _movie_preview_images_pattern = re.compile(r'<img src="([^"]+)"')
    
    _movie_download_section_pattern = re.compile(r'<article(.*?)</article>', re.DOTALL)
    _movie_download_item_pattern = re.compile(r'<a(.*?)</a>', re.DOTALL)
    _movie_size_pattern = re.compile(r'<span class="meta">[^\d]*([\d]+(\.[\d]+)?[TGMK]{1}B)')
    _movie_magnet_pattern = re.compile(r'href="(magnet:[^"]+)"')

    @staticmethod
    def parse_movie_title(page):
        pats = re.findall(MovieParser._movie_title_pattern, page)
        if len(pats) > 0:
            return pats[0].strip()
        return None
    
    @staticmethod
    def parse_movie_cover(page):
        pats = re.findall(MovieParser._movie_cover_pattern, page)
        if len(pats) > 0:
            return pats[0].strip()
        return ''

    @staticmethod
    def parse_movie_tags(page):
        tags = []
        section_pats = re.findall(MovieParser._movie_tag_section_pattern, page)
        if len(section_pats) > 0:
            section_content = section_pats[0]
            pats = re.findall(MovieParser._movie_tags_pattern, section_content)
            for pat in pats:
                tags.append({
                    'url': pat[0].strip(),
                    'name': pat[1].strip()
                })
        return tags
    
    @staticmethod
    def parse_movie_actors(page):
        actors = []
        section_pats = re.findall(MovieParser._movie_actors_section_pattern, page)
        if len(section_pats) > 0:
            section_content = section_pats[0]
            pats = re.findall(MovieParser._movie_actors_pattern, section_content)
            for pat in pats:
                actors.append({
                    'url': pat[0].strip(),
                    'name': pat[1].strip()
                })
        return actors
    
    @staticmethod
    def parse_movie_vol(page):
        section_pats = re.findall(MovieParser._movie_vol_section_pattern, page)
        if len(section_pats) > 0:
            section_content = section_pats[0]
            pats = re.findall(MovieParser._movie_vol_pattern, section_content)
            if len(pats) > 0:
                return {
                    'url': pats[0][0].strip(),
                    'ser': pats[0][1].strip(),
                    'vol': pats[0][1].strip() + pats[0][2].strip(),
                }
        return None
    
    @staticmethod
    def parse_movie_date(page):
        section_pats = re.findall(MovieParser._movie_date_section_pattern, page)
        if len(section_pats) > 0:
            section_content = section_pats[0]
            pats = re.findall(MovieParser._movie_date_pattern, section_content)
            if len(pats) > 0:
                return pats[0]
        return ''

    @staticmethod
    def parse_movie_preview(page):
        ret = {
            'v': [],
            'i': []
        }
        section_pats = re.findall(MovieParser._movie_preview_section_pattern, page)
        if len(section_pats) > 0:
            section_content =  section_pats[0]
            video_pats = re.findall(MovieParser._movie_preview_videos_pattern, section_content)
            if len(video_pats) > 0:
                ret['v'] = []
                for url in video_pats:
                    ret['v'].append(f'https:{url}' if not url.startswith('http') else url) 
            image_pats = re.findall(MovieParser._movie_preview_images_pattern, section_content)
            if len(image_pats) > 0:
                ret['i'] = image_pats
        return ret
    
    @staticmethod
    def parse_movie_download(page):
        ret = []
        section_pats = re.findall(MovieParser._movie_download_section_pattern, page)
        if len(section_pats) > 1:
            section_content =  section_pats[1]
            items_pats = re.findall(MovieParser._movie_download_item_pattern, section_content)
            for item in items_pats:
                magnet_pats = re.findall(MovieParser._movie_magnet_pattern, item)
                if len(magnet_pats) > 0:
                    download_item = {
                        'mag': magnet_pats[0],
                    }
                    size_pats = re.findall(MovieParser._movie_size_pattern, item)
                    if len(size_pats) > 0:
                        download_item['size'] = size_pats[0][0]
                    ret.append(download_item)
        return ret

class MovieHelper:

    DEFAULT_MOVIE_DB_DIR='javdb/movies'

    @staticmethod
    def get_movie_dir(movie_db, movie_id, mkdir=False):
        dir = os.path.join(movie_db, movie_id[:2].lower() if len(movie_id) > 1 else movie_id)
        if mkdir:
            os.makedirs(dir, exist_ok=True)
        return dir

    @staticmethod
    def get_movie_file(movie_db, movie_id, mkdir=False):
        return os.path.join(MovieHelper.get_movie_dir(movie_db, movie_id, mkdir), f'{movie_id}.json')

    @staticmethod
    def save_movie(movie, movie_db=DEFAULT_MOVIE_DB_DIR):
        movie_id = movie['id']
        file = MovieHelper.get_movie_file(movie_db, movie_id, mkdir=True)
        with open(file, 'w') as f:
            json.dump(movie, f, indent=2, ensure_ascii=False)
            f.write('\n')

    @staticmethod
    def load_movie(movie_id, movie_db=DEFAULT_MOVIE_DB_DIR):
        file = MovieHelper.get_movie_file(movie_db, movie_id)
        if not os.path.exists(file):
            return None
        with open(file, 'r') as f:
            movie = json.load(f)
        return movie

    @staticmethod
    def scan_movie_ids_indb(movie_db=DEFAULT_MOVIE_DB_DIR):
        if not os.path.exists(movie_db):
            os.makedirs(movie_db)

        movie_ids = []
        for root, dirs, files in os.walk(movie_db):
            for file in files:
                if file.endswith('.json'):
                    movie_id = file[:-5]
                    movie_ids.append(movie_id)
        logging.info(f'Loaded {len(movie_ids)} movie_ids')
        return movie_ids

    @staticmethod
    def get_movie_cover_dir(data_dir, movie_id, mkdir=False):
        dir = MovieHelper.get_movie_dir(os.path.join(data_dir, 'covers'), movie_id)
        if mkdir:
            os.makedirs(dir, exist_ok=True)
        return dir

    @staticmethod
    def get_movie_cover_file(data_dir, movie_id, cover_file_ext, mkdir=False):
        return os.path.join(MovieHelper.get_movie_cover_dir(data_dir, movie_id, mkdir=mkdir), f'{movie_id}.{cover_file_ext}')

    @staticmethod
    def get_movie_previews_dir(data_dir, movie_id, mkdir=False):
        dir = MovieHelper.get_movie_dir(os.path.join(data_dir, 'previews'), movie_id)
        dir = os.path.join(dir, movie_id)
        if mkdir:
            os.makedirs(dir, exist_ok=True)
        return dir

    @staticmethod
    def get_movie_previews_file(data_dir, movie_id, preview_file, mkdir=False):
        return os.path.join(MovieHelper.get_movie_previews_dir(data_dir, movie_id, mkdir=mkdir), f'{preview_file}')

    @staticmethod
    def pull_movie_page(url):
        url_root = UrlParser.parse_url_root(url)
        movie_id = UrlParser.parse_movie_id(url)
        movie_url = UrlParser.get_movie_url(url_root, movie_id)
        parser_logger.debug(movie_url)
        res = http_get(movie_url)
        if not res:
            return None
        page = res
        # parser_logger.debug(page)
        movie = {
            'id': movie_id,
            'url': movie_url,
        }

        movie_cover = MovieParser.parse_movie_cover(page)
        parser_logger.debug(f'cover: {movie_cover}')
        movie['cover'] = movie_cover

        movie_title = MovieParser.parse_movie_title(page)
        parser_logger.debug(f'title: {movie_title}')
        if not movie_title:
            return None
        movie['title'] = movie_title

        movie_tags = MovieParser.parse_movie_tags(page)
        parser_logger.debug(f'tags: {movie_tags}')
        movie['tags'] = movie_tags

        movie_actors = MovieParser.parse_movie_actors(page)
        parser_logger.debug(f'actors: {movie_actors}')
        movie['actors'] = movie_actors

        movie_vol = MovieParser.parse_movie_vol(page)
        parser_logger.debug(f'vol: {movie_vol}')
        movie['vol'] = movie_vol
        if not movie_vol:
            parser_logger.error(f'failed to parse movie_vol of url: {url}')

        movie_date = MovieParser.parse_movie_date(page)
        parser_logger.debug(f'date: {movie_date}')
        movie['date'] = movie_date

        movie_previews = MovieParser.parse_movie_preview(page)
        parser_logger.debug(f'previews: {movie_previews}')
        movie['previews'] = movie_previews

        movie_download = MovieParser.parse_movie_download(page)
        parser_logger.debug(f'download: {movie_download}')
        movie['downloads'] = movie_download

        return movie
    
    @staticmethod
    def count_movies():
        movie_ids = MovieHelper.scan_movie_ids_indb()
        return len(movie_ids)

class SearchParser:

    _search_movie_list_section_pattern = re.compile(r'<div class="movie-list(.*?)</section>', re.DOTALL)
    _search_movie_item_pattern = re.compile(r'<div class="item">(.*?)</a>', re.DOTALL)
    _search_movie_item_vol_pattern = re.compile(r'<strong>(.*?)</strong>')
    _search_movie_url_title_pattern = re.compile(r'<a href="([^"]+)".*?title="([^"]+)"')
    _search_movie_cover_pattern = re.compile(r'src="([^"]+)"')
    _search_movie_date_pattern = re.compile(r'([\d]{2})/([\d]{2})/([\d]{4})')

    @staticmethod
    def search_movie_by_vol(vol, url_root=UrlParser.DEFAULT_URL_ROOT):
        logging.info(f'[+] Searching movie with vol={vol}')
        url = UrlParser.get_search_url(vol, url_root)
        parser_logger.debug(url)
        page = http_get(url)
        if not page:
            return None
        
        movie_list_section_pats = re.findall(SearchParser._search_movie_list_section_pattern, page)
        if len(movie_list_section_pats) > 0:
            section_content = movie_list_section_pats[0]
            movie_item_pats = re.findall(SearchParser._search_movie_item_pattern, section_content)
            for item in movie_item_pats:
                vol_pats = re.findall(SearchParser._search_movie_item_vol_pattern, item)
                if len(vol_pats) > 0:
                    if vol_pats[0].upper() == vol.upper():
                        url_title_pats = re.findall(SearchParser._search_movie_url_title_pattern, item)
                        if len(url_title_pats) > 0:
                            url, title = url_title_pats[0]
                            cover_pats = re.findall(SearchParser._search_movie_cover_pattern, item)
                            if len(cover_pats) > 0:
                                cover = cover_pats[0]
                            else:
                                cover = ''
                            date_pats = re.findall(SearchParser._search_movie_date_pattern, item)
                            if len(date_pats) > 0:
                                date = f'{date_pats[0][2]}-{date_pats[0][0]}-{date_pats[0][1]}'
                            else:
                                date = '1970-01-01'
                            return {
                                'id': UrlParser.parse_movie_id(url),
                                'url': UrlParser.get_full_url(url_root, url),
                                'title': title.strip(),
                                'cover': cover,
                                'vol': vol_pats[0],
                                'date': date,
                            }

        return None

class MovieSeries:

    DEFAULT_SERIES_DB_DIR='javdb/series'

    @staticmethod
    def parse_series_from_vol(vol):
        vol = vol.replace(' ', '')
        if '-' in vol:
            return vol.split('-')[0].upper()
        elif '_' in vol:
            return vol.split('_')[0].upper()
        elif '.' in vol:
            return vol.split('.')[0].upper()
        elif len(vol) > 4:
            return vol[:4].upper()
        else:
            return vol.upper()

    @staticmethod
    def save_movie_summary(movie_summary, series_db=DEFAULT_SERIES_DB_DIR):
        if not movie_summary.get('vol', None):
            return
        series = MovieSeries.parse_series_from_vol(movie_summary["vol"])
        dir = os.path.join(series_db, series)
        if not os.path.exists(dir):
            os.makedirs(dir)
        file = os.path.join(dir, f'{movie_summary["vol"]}.json')
        with open(file, 'w') as f:
            json.dump(movie_summary, f, indent=2, ensure_ascii=False)
            f.write('\n')

    @staticmethod
    def load_movie_summary(vol, series_db=DEFAULT_SERIES_DB_DIR):
        series = MovieSeries.parse_series_from_vol(vol)
        dir = os.path.join(series_db, series)
        file = os.path.join(dir, f'{vol}.json')
        if not os.path.exists(file):
            return None
        with open(file, 'r') as f:
            movie_summary = json.load(f)
        return movie_summary

    @staticmethod
    def import_movie_details(ignore_exists=True):
        movie_ids = MovieHelper.scan_movie_ids_indb()
        for movie_id in movie_ids:
            movie = MovieHelper.load_movie(movie_id)
            if not movie:
                continue
            if not movie.get('vol', None):
                continue
            vol = movie['vol']['vol']

            if ignore_exists and MovieSeries.load_movie_summary(vol):
                continue
            
            movie_summary = {
                'id': movie_id,
                'url': movie['url'],
                'cover': movie['cover'],
                'title': movie['title'],
                'date': movie['date'],
                'vol': vol,    
            }
            MovieSeries.save_movie_summary(movie_summary)

    @staticmethod
    def import_actor_movies(ignore_exists=True):
        actors = ActorHelper.load_actors()
        for actor in actors.values():
            if actor['movies']:
                url_root = UrlParser.parse_url_root(actor['url'])
                for movie_id in actor['movies']:
                    movie = actor['movies'][movie_id]
                    if not movie.get('vol', None):
                        continue
                    vol = movie['vol']

                    if ignore_exists and MovieSeries.load_movie_summary(vol):
                        continue

                    movie_summary = {
                        'id': movie_id,
                        'url': UrlParser.get_full_url(url_root,  movie['url']),
                        'cover': movie['cover'],
                        'title': movie['title'],
                        'date': movie['date'],
                        'vol': vol
                    }
                    MovieSeries.save_movie_summary(movie_summary)

    @staticmethod
    def scan_vols_indb(series_db=DEFAULT_SERIES_DB_DIR):
        if not os.path.exists(series_db):
            os.makedirs(series_db)

        vols = []
        for root, dirs, files in os.walk(series_db):
            for file in files:
                if file.endswith('.json'):
                    vol = file[:-5]
                    vols.append(vol)
        logging.info(f'Loaded {len(vols)} vols')
        return vols
    
    @staticmethod
    def load_or_search_vol(vol, save=True):
        movie_summary = MovieSeries.load_movie_summary(vol)
        if movie_summary:
            return movie_summary
        else:
            movie_summary = SearchParser.search_movie_by_vol(vol)
            if save and movie_summary:
                MovieSeries.save_movie_summary(movie_summary)
            return movie_summary
        
    @staticmethod
    def count_vols():
        movie_vols = MovieSeries.scan_vols_indb()
        return len(movie_vols)

# fetch & save actor's summary & movies
# get_new_only=True, fetch new movies only
def update_actor_urls(urls=[], get_new_only=True):
    logging.info(f'Updating {len(urls)} actor urls: get_new_only={get_new_only}')
    total_urls = len(urls)
    cnt = 0
    todo_urls = []
    for url in urls:
        cnt = cnt + 1
        actor_id = UrlParser.parse_actor_id(url)
        logging.info(f'[{cnt}/{total_urls}] Processing Actor: {actor_id}')
        actor = ActorHelper.pull_actor_page(url, useOriginUrl=False, actor=ActorHelper.load_actor(actor_id) if get_new_only else None)
        if actor:
            # print(json.dumps(actor, indent=2, ensure_ascii=False))
            ActorHelper.print_actor_summary(ActorHelper.update_save_actor(actor))
        else:
            todo_urls.append(url)
    return todo_urls

# call update_actor_urls() and save failed urls to file.
# new_actor_only=True, only update on new actors, not already saved ones.
def add_actor_urls(urls=[], new_actor_only=True):
    logging.info(f'Adding {len(urls)} actor urls: new_actor_only={new_actor_only}')
    if not new_actor_only:
        todo_urls = urls
    else:
        todo_urls = []
        for url in urls:
            actor_id = UrlParser.parse_actor_id(url)
            actor = ActorHelper.load_actor(actor_id)
            if not actor:
                todo_urls.append(url)
    logging.info(f'Got {len(todo_urls)} actor urls TODO:')
    failed_urls = update_actor_urls(todo_urls, get_new_only=False)
    if len(failed_urls) > 0:
        failed_urls_file = f'failed_actor_urls_{int(time.time())}.txt'
        write_plain_urls(failed_urls, failed_urls_file)
        logging.warn(f'Add actor urls failed: {failed_urls}, saved to {failed_urls_file}')
    else:
        logging.info(f'Successfully added {len(todo_urls)} actor urls')
    return failed_urls

# pull movie page and save to db
def update_movie_urls(urls=[], get_new_only=True):
    logging.info(f'Updating {len(urls)} movie urls: get_new_only={get_new_only}')
    total_urls = len(urls)
    cnt = 0
    todo_urls = []
    for url in urls:
        cnt = cnt + 1
        movie_id = UrlParser.parse_movie_id(url)
        logging.info(f'[{cnt}/{total_urls}] Processing Movie: {movie_id}')
        movie_detail = MovieHelper.pull_movie_page(url)
        if movie_detail:
            logging.info(f'MovieDetail Got: {len(movie_detail["tags"])} tags, {len(movie_detail["previews"]["i"])} images, {len(movie_detail["previews"]["v"])} videos, {len(movie_detail["downloads"])} downloads')
            MovieHelper.save_movie(movie_detail)
        else:
            todo_urls.append(url)
    return todo_urls

# pull movie page and save to db
def add_movie_urls(urls=[], new_movie_only=True):
    logging.info(f'Adding {len(urls)} movie urls: new_movie_only={new_movie_only}')
    if not new_movie_only:
        todo_urls = urls
    else:
        todo_urls = []
        for url in urls:
            movie_id = UrlParser.parse_movie_id(url)
            movie = MovieHelper.load_movie(movie_id)
            if not movie:
                todo_urls.append(url)
    logging.info(f'Got {len(todo_urls)} movie urls TODO:')
    failed_urls = update_movie_urls(todo_urls, get_new_only=False)
    if len(failed_urls) > 0:
        failed_urls_file = f'failed_movie_urls_{int(time.time())}.txt'
        write_plain_urls(failed_urls, failed_urls_file)
        logging.warn(f'Add movie urls failed: {failed_urls}, saved to {failed_urls_file}')
    else:
        logging.info(f'Successfully added {len(todo_urls)} movie urls')
    return failed_urls

# add new actor or movie url to db
def add_new_urls(urls=[]):
    actor_urls = []
    movie_urls = []
    unknown_urls = []
    for url in urls:
        if UrlParser.is_movie_url(url):
            movie_urls.append(url)
        elif UrlParser.is_actor_url(url):
            actor_urls.append(url)
        else:
            unknown_urls.append(url)

    add_actor_urls(actor_urls)
    add_movie_urls(movie_urls)
    if len(unknown_urls) > 0:
        print(f'[X] Unknown URLs: {len(unknown_urls)}')
        print(unknown_urls)

# fetch new movies of actors in db
def update_actors_indb(db_dir=ActorHelper.DEFAULT_ACTOR_DB_DIR):
    logging.info(f'Updating actors in db: {db_dir}')
    actors = ActorHelper.load_actors(db_dir)
    urls = []
    for actor in actors.values():
        urls.append(actor['url'])
    update_actor_urls(urls, get_new_only=True)

def do_update_movie_detail_from_summary(movie, url_root=None, progress_str='', movie_db_dir=MovieHelper.DEFAULT_MOVIE_DB_DIR):
    movie_url = UrlParser.get_full_url(url_root, movie['url'])
    movie['cover'] = UrlParser.get_full_url(url_root, movie['cover'])
    logging.info(
        f'Processing {progress_str}: {movie["vol"]}({movie["id"]})')
    movie_detail = MovieHelper.pull_movie_page(movie_url)
    if movie_detail:
        logging.info(f'MovieDetail Got: {len(movie_detail["tags"])} tags, {len(movie_detail["previews"]["i"])} images, {len(movie_detail["previews"]["v"])} videos, {len(movie_detail["downloads"])} downloads')
        MovieHelper.save_movie(movie_detail, movie_db_dir)
    return movie_detail

# fetch detail of movies of actors in db
def update_actor_movies_indb(db_dir=ActorHelper.DEFAULT_ACTOR_DB_DIR, movie_db_dir=MovieHelper.DEFAULT_MOVIE_DB_DIR, ignore_exists=True):
    logging.info(f'Updating actor movies in db: {db_dir}')
    actors = ActorHelper.load_actors(db_dir)
    cnt_actor = 0
    total_actor = len(actors)
    todo_actor_urls = {}
    for actor_id in actors:
        actor = actors[actor_id]
        cnt_actor = cnt_actor + 1
        logging.info(f'Processing Actor [{cnt_actor}/{total_actor}]: {actor_id}-{actor["summary"]["name"]}')
        cnt_movie = 0
        total_movie = len(actor['movies'])
        for movie_id in actor['movies']:
            cnt_movie = cnt_movie + 1
            movie = MovieHelper.load_movie(movie_id, movie_db=movie_db_dir)
            if movie and ignore_exists:
                continue
            movie = actor['movies'][movie_id]
            url_root = UrlParser.parse_url_root(actor['url'])
            do_update_movie_detail_from_summary(movie, url_root, progress_str=f'Movie [{cnt_movie}/{total_movie}], Actor [{cnt_actor}/{total_actor}]', movie_db_dir=movie_db_dir)
        logging.info(f'Done {total_movie} Movies : {actor_id}-{actor["summary"]["name"]}')

    movie_vols = MovieSeries.scan_vols_indb()
    cnt_movie = 0
    total_movie = len(movie_vols)
    for vol in movie_vols:
        cnt_movie = cnt_movie + 1
        movie_summary = MovieSeries.load_movie_summary(vol)
        movie = MovieHelper.load_movie(movie_summary['id'])
        if movie and ignore_exists:
            continue
        do_update_movie_detail_from_summary(movie_summary, progress_str=f'Movie [{cnt_movie}/{total_movie}]', movie_db_dir=movie_db_dir)

    return list(todo_actor_urls.values())

# download covers of movie or movie_summary
def do_download_movie_cover(movie_or_summary, target_dir, url_root=None, progress_str=''):
    movie_cover_url = UrlParser.get_full_url(url_root if url_root else UrlParser.parse_url_root(movie_or_summary['url']), movie_or_summary['cover'])
    movie_cover_file_ext =  UrlParser.parse_file_ext(
        UrlParser.parse_url_file(movie_cover_url))
    movie_cover_file = MovieHelper.get_movie_cover_file(target_dir, movie_or_summary['id'], movie_cover_file_ext, True)
    if not os.path.exists(movie_cover_file):
        logging.info(
            f'Downloading {progress_str}: {movie_cover_url} to {movie_cover_file}')
        http_download(movie_cover_url, movie_cover_file, remove_if_err=True)

# download avatars of actors & covers of movie summaries
def download_covers(target_dir='javdb-data'):
    logging.info(f'Downloading avatars & covers to folder: {target_dir}')
    
    actors = ActorHelper.load_actors()
    actor_cnt = 0
    actor_total = len(actors)
    for actor_id in actors:
        actor_cnt = actor_cnt + 1
        actor = actors[actor_id]
        url_root = UrlParser.parse_url_root(actor['url'])
        logging.info(f'Processing Actor [{actor_cnt}/{actor_total}]: {actor_id}-{actor["summary"]["name"]}')

        avatar_url = UrlParser.get_full_url(url_root, actor['summary']['avatar'])
        avatar_file_ext = UrlParser.parse_file_ext(
            UrlParser.parse_url_file(avatar_url))
        avatar_file = ActorHelper.get_actor_avatar_file(target_dir, actor_id, avatar_file_ext, True)

        if not os.path.exists(avatar_file):
            logging.info(
                f'Downloading avatar [{actor_cnt}/{actor_total}]: {avatar_url} to {avatar_file}')
            http_download(avatar_url, avatar_file, remove_if_err=True)

        movie_cnt = 0
        movie_total = len(actor['movies'])
        for movie_id in actor['movies']:
            movie_cnt = movie_cnt + 1
            movie = actor['movies'][movie_id]
            do_download_movie_cover(movie, target_dir, url_root, progress_str=f'cover [{movie_cnt}/{movie_total}], actor [{actor_cnt}/{actor_total}]')

    movie_ids = MovieHelper.scan_movie_ids_indb()
    movie_cnt = 0
    movie_total = len(movie_ids)
    for movie_id in movie_ids:
        movie_cnt = movie_cnt + 1
        movie = MovieHelper.load_movie(movie_id)
        if not movie:
            continue
        do_download_movie_cover(movie, target_dir, progress_str=f'cover [{movie_cnt}/{movie_total}]')

    movie_vols = MovieSeries.scan_vols_indb()
    movie_cnt = 0
    movie_total = len(movie_vols)
    for vol in movie_vols:
        movie_cnt = movie_cnt + 1
        movie_summary = MovieSeries.load_movie_summary(vol)
        if not movie_summary:
            continue
        do_download_movie_cover(movie_summary, target_dir, progress_str=f'cover [{movie_cnt}/{movie_total}]')

# download previews(video & images) of movie details
def download_movie_previews(movie_db=MovieHelper.DEFAULT_MOVIE_DB_DIR, target_dir='javdb-data', threads=1):

    if threads == 1:
        single_thread = True
    else:
        single_thread = False
    logging.info(f'Loading movies from {movie_db}')
    movie_ids = MovieHelper.scan_movie_ids_indb()
    movie_total = len(movie_ids)
    if single_thread:
        preview_total = 0
    else:
        preview_total = 0#load_movie_vol_previews_total(movie_db=movie_db)
    logging.info(
        f'Start downloading {preview_total} previews of {movie_total} movies to folder: {target_dir} with {threads} thread(s)')
    movie_cnt = 0
    preview_cnt = 0
    processed_previews = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads, thread_name_prefix='previews_') as executor:
        global default_executor
        default_executor = executor
        signal.signal(signal.SIGINT, signal_handler)

        if not single_thread:
            pbar = tqdm(total=movie_total, unit='movie',
                        desc='Processing movies')
            # pbar2 = tqdm(total=preview_total, unit='preview', desc='Processing previews')
            pbar2 = tqdm(iterable=True, unit='preview', desc='Processing previews')
        else:
            pbar = None
            pbar2 = None

        for movie_id in movie_ids:
            movie = MovieHelper.load_movie(movie_id)
            if not movie:
                continue
            movie_cnt = movie_cnt + 1
            added_previews = preview_cnt
            preview_cnt += len(movie['previews']['i']) + len(movie['previews']['v'])
            if pbar:
                pbar.update(1)
                # pbar2.update(len(movie['preview_images']) +
                #             len(movie['preview_videos']))

            while not executor._shutdown and executor._work_queue.qsize() > threads * 3:
                time.sleep(1)
                # logging.info(f'shutdown: {executor._shutdown}, queue: {executor._work_queue.qsize()}')
                if pbar2:
                    processed_now = added_previews - executor._work_queue.qsize()
                    if processed_now > processed_previews:
                        pbar2.update(processed_now - processed_previews)
                        processed_previews = processed_now
                

            if executor._shutdown:
                break

            url_root = UrlParser.parse_url_root(movie['url'])

            for image_url in movie['previews']['i']:
                url = UrlParser.get_full_url(url_root, image_url)
                url = url.replace('_s_', '_l_')
                filename = UrlParser.parse_url_file(url)
                target_file = MovieHelper.get_movie_previews_file(target_dir, movie_id, filename, True)
                if not os.path.exists(target_file):
                    executor.submit(http_download, url, target_file, log_start=f'[{movie_cnt}/{movie_total}]Downloading preview image: {url} to {target_file}' if single_thread else None, remove_if_err=True, show_progress=False)

            for video_url in movie['previews']['v']:
                url = UrlParser.get_full_url(url_root, video_url)
                filename = UrlParser.parse_url_file(url)
                target_file = MovieHelper.get_movie_previews_file(target_dir, movie_id, filename, True)
                if not os.path.exists(target_file):
                    executor.submit(http_download, url, target_file, log_start=f'[{movie_cnt}/{movie_total}]Downloading preview video: {url} to {target_file}' if single_thread else None, remove_if_err=True, show_progress=single_thread)

        executor.shutdown(wait=True)
        if default_executor == executor:
            default_executor = None
        logging.info(f'Finished downloading {preview_cnt} previews')

# sync db..
def sync_indb():
    # update_actor_urls(pull_ranks(), get_new_only=True)
    update_actors_indb()
    todo_urls = update_actor_movies_indb()
    while len(todo_urls) > 0:
        logging.info(f'Todo urls: {len(todo_urls)}')
        add_actor_urls(todo_urls)
        todo_urls = update_actor_movies_indb()
    download_covers()
    # download_movie_previews() # too large, not necessary

# import MovieSeries from other db
def import_movie_series():
    logging.info(f'Importing MovieSeries from Actors.')
    MovieSeries.import_actor_movies()
    logging.info(f'Importing MovieSeries from Movies.')
    MovieSeries.import_movie_details()
    logging.info(f'Import DONE!')
    vols = MovieSeries.scan_vols_indb()

def count():
    actor_cnt = ActorHelper.count_actors()
    movie_cnt = MovieHelper.count_movies()
    vol_cnt = MovieSeries.count_vols()
    logging.info(f'[=] Actor count: {actor_cnt}')
    logging.info(f'[=] Movie count: {movie_cnt}')
    logging.info(f'[=] Vol count: {vol_cnt}')

def test():
    parser_logger.setLevel(logging.DEBUG)
    http_logger.setLevel(logging.DEBUG)
    # ActorHelper.pull_actor_page('https://javdb.com/actors/B8K29')
    # add_actor_urls(urls=['https://javdb.com/actors/B8K29', 'https://javdb.com/actors/Av2e'], new_actor_only=True)
    # update_actors_indb()
    # MovieDetailHelper.pull_movie_page('https://javdb.com/v/meN5wM')
    # MovieDetailHelper.pull_movie_page('https://javdb.com/v/Az3vBq')
    # update_actor_movies_indb()
    movie = SearchParser.search_movie_by_vol('SSIS-999')
    print(movie)
    pass
    # cnt = 0
    # while True:
    #     cnt += 1

def print_usage():
    print('[==] Usage: javdb [actors | movies | covers | previews | sync]')
    print('[==] Urls: javdb [{actor_url} | {movie_url} | {*.txt}')
    print('[==] Add: javdb [parser | import_movie_series | search | vol | count]')

if __name__ == '__main__':
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    data_dir = 'javdb-data'

    if len(sys.argv) > 1:
        arg = sys.argv[1].strip()
        print(f'[==] Parsed arg: {arg}')
        if arg.startswith('covers'): # download avatars & covers
            if len(sys.argv) > 2:
                data_dir = sys.argv[2].strip()
            print(f'[=] Start downloading covers to {data_dir}')
            download_covers(target_dir=data_dir)
        elif arg.startswith('previews'): # download movie previews for movies in db
            data_dir = '/Volumes/Download-2T/javdb-data'
            if len(sys.argv) > 2:
                data_dir = sys.argv[2].strip()
            print(f'[=] Start downloading movie previews to {data_dir}')
            download_movie_previews(target_dir=data_dir, threads=5)        
        elif arg.startswith('actors'): # update actors in db, pull actor & movie info from actor page
            print(f'[=] Start updating actors')
            update_actors_indb()
        elif arg.startswith('movies'): # update actors(in db)' movie info, pull detail from movie page 
            print(f'[=] Start updating movies')
            update_actor_movies_indb()
        elif arg.startswith('sync'): # sync actors and movies indb, download covers
            print(f'[=] Start syncing db')
            sync_indb()
        elif arg.startswith("http"): # pull actor page, update & save
            print(f'[=] Start with input URL: {arg}')
            add_new_urls([arg])
        elif arg.endswith('.txt'): # pull actors' pages, update & save
            print(f'[=] Start with input file: {arg}')
            add_new_urls(read_plain_urls(arg))
        elif arg.startswith('import_movie_series'): # build index
            import_movie_series()
        elif arg.startswith('search'): # search movie
            if len(sys.argv) > 2:
                vol = sys.argv[2].strip()
                print(f'[=] Searching vol: {vol}')
                movie = SearchParser.search_movie_by_vol(vol)
                if not movie:
                    print(f'[X] No movie found for vol: {vol}')
                else:
                    print(json.dumps(movie, indent=2, ensure_ascii=False))
                    print(f'[=] Search DONE!')
            else:
                print('[X] search keyword needed!')
        elif arg.startswith('vol'): # print movie by vol
            if len(sys.argv) > 2:
                vol = sys.argv[2].strip().upper()
                print(f'[=] Loading vol: {vol}')
                movie_summary = MovieSeries.load_or_search_vol(vol)
                print(json.dumps(movie_summary, indent=2, ensure_ascii=False))
            else:
                print('[X] vol value needed!')
        elif arg.startswith('count'): # count actors, movies, vols
            count()
        elif arg.startswith('test'): # test
            test()
        elif arg.startswith('help'): # help
            print_usage()
        else:
            print(f'[X] Unsupported input arg.')
            print_usage()
    else:
       print_usage()

    close_chrome()