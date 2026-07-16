import asyncio
from collections.abc import AsyncGenerator, Callable
import logging
from pathlib import Path
import os
from argparse import ArgumentParser
from functools import partial

from bs4 import BeautifulSoup
from httpx import AsyncClient, HTTPStatusError, Limits, Response, ReadTimeout
import aiofiles
from aiofiles.ospath import exists as async_path_exists
from rich.progress import Progress, MofNCompleteColumn

BASE_URL = "https://2e.aonprd.com/"
BASE_DOWNLOAD_DIR = Path("download")

ALL_CATEGORIES = {
    'Ancestries': (200, 64),
    'Classes': (100, 32),
    'Archetypes': (500, 64),
    'Equipment': (10000, 256),
    'Armor': (100, 32),
    'Shields': (100, 32),
    'Weapons': (500, 64),
    'NPCs': (2000, 64),
    'Monsters': (5000, 128),
    'Deities': (1000, 128)
}

MAX_CONCURRENT_REQUESTS = 20
CLIENT = AsyncClient(
    follow_redirects=True,
    limits=Limits(
        max_connections=MAX_CONCURRENT_REQUESTS * 2,
        max_keepalive_connections=MAX_CONCURRENT_REQUESTS * 2,
        keepalive_expiry=10
    ),
    timeout=30,
)
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

PAGE_VALIDITY_CACHE = {}

logging.basicConfig(filename="archivist.log", filemode='w', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.CRITICAL)


async def main():
    parser = ArgumentParser(prog="nethys-archivist")
    parser.add_argument('-c', '--count', action='store_true', help="Instead of downloading, only display the number of pages that will be scraped and checked for images in the specified categories.")
    parser.add_argument('categories', nargs='*', help="The categories of entry to download images from (Ancestries, Classes, Equipment...) If not specified, every category will be downloaded.")

    args = parser.parse_args()

    if args.categories:
        categories = [cat.title() for cat in args.categories]
    else:
        categories = list(ALL_CATEGORIES.keys())

    if args.count:
        logging.info("Running in category count mode")
        for category in categories:
            min_id, max_id = await get_category_id_bounds(category)
            print(f"{category}: ~{max_id - min_id + 1}")
    else:
        logging.info("Running in download mode")
        await download_multiple_categories(categories)
    
    await CLIENT.aclose()


async def download_multiple_categories(categories: list[str], overwrite_if_exists: bool = False):
    if not os.path.exists(BASE_DOWNLOAD_DIR):
        logging.info(f"Creating directory {BASE_DOWNLOAD_DIR}")
        os.mkdir(BASE_DOWNLOAD_DIR)

    with Progress(*Progress.get_default_columns(), MofNCompleteColumn()) as progress:
        downloads = []
        for category in categories:
            task_scan = progress.add_task(f"[yellow][{category}] Scanning entries...", total=None)
            task_scrape = progress.add_task(f"[light_green][{category}] Scraping image links...", visible=False, total=0)
            task_download = progress.add_task(f"[cyan][{category}] Downloading images...", visible=False, total=0)
            downloads.append(download_category(
                category,
                overwrite_if_exists=overwrite_if_exists,
                update_scan=partial(progress.update, task_scan),
                update_scrape=partial(progress.update, task_scrape),
                update_download=partial(progress.update, task_download)
            ))
        
        await asyncio.gather(*downloads)


async def download_category(
    category: str,
    overwrite_if_exists: bool = False,
    update_scan: Callable | None = None,
    update_scrape: Callable | None = None,
    update_download: Callable | None = None
):
    category_dir = BASE_DOWNLOAD_DIR/category
    if not os.path.exists(category_dir):
        logging.info(f"Creating directory {category_dir}")
        os.mkdir(category_dir)

    min_id, max_id = await get_category_id_bounds(category)

    if update_scan:
        update_scan(visible=False)
    if update_scrape:
        update_scrape(visible=True, total=max_id - min_id + 1, completed=0)
    
    await download_thumbnails_in_range(
        category_dir,
        category,
        min_id,
        max_id,
        overwrite_if_exists=overwrite_if_exists,
        update_scrape=update_scrape,
        update_download=update_download
    )


async def get_category_id_bounds(category) -> tuple[int, int]:
    max_cap, step = ALL_CATEGORIES.get(category, (10000, 128))

    min_id = await search_min_id(category, max_id=max_cap, initial_step=step)
    max_id = await search_max_id(category, min_id=min_id, max_id=max_cap, initial_step=step)

    return min_id, max_id


async def download_thumbnails_in_range(
    base_path: Path | str,
    category: str,
    start_id: int,
    end_id: int,
    overwrite_if_exists: bool = False,
    update_scrape: Callable | None = None,
    update_download: Callable | None = None
):
    base_path = Path(base_path)
    
    downloads = []
    n_downloads = 0
    async for url in scrape_thumbnail_urls(category, start_id, end_id, tick_progress=partial(update_scrape, advance=1)):
        filename = url.split('/')[-1]
        path = base_path/filename

        downloads.append(download_image(url, path, overwrite_if_exists=overwrite_if_exists, tick_progress=partial(update_download, advance=1)))
        n_downloads += 1
        update_download(visible=True, total=n_downloads)
    
    update_scrape(visible=False)
    
    await asyncio.gather(*downloads)


async def download_image(url: str, path: Path | str, overwrite_if_exists: bool = False, tick_progress: Callable | None = None):
    path = Path(path)

    if await async_path_exists(path):
        if overwrite_if_exists:
            logging.info(f"Overwriting existing file at {path}")
        else:
            logging.info(f"Skipping existing file at {path}")
            tick_progress()
            return
    
    async with SEMAPHORE:
        response = await CLIENT.get(url)

    if response.is_error:
        logging.error(f"Error downloading image at {url} ({response.status_code})")
        return

    async with aiofiles.open(path, 'wb') as fp:
        for chunk in response.iter_bytes(chunk_size=8192):
            await fp.write(chunk)

    logging.info(f"Downloaded {url} to {path}")
    tick_progress()
    #print(path)


async def search_min_id(category: str, min_id: int = 1, max_id: int = 10000, initial_step=128) -> int:
    if await does_entry_exist(category, min_id):
        return min_id

    step = initial_step
    while step > 0:
        if min_id + step <= max_id and await does_entry_exist(category, min_id + step):
            step //= 2
        else:
            min_id += step
    
    return min_id + 1


async def search_max_id(category: str, min_id: int = 1, max_id: int = 10000, initial_step=128) -> int:
    # if min_id == 1:
    #     bin_result = await binary_search_max_id(category, min_id=min_id, max_id=max_id)
    #     if bin_result > min_id:
    #         return bin_result
    
    step = initial_step
    while step > 0:
        if max_id - step >= min_id and await does_entry_exist(category, max_id - step):
            step //= 2
        else:
            max_id -= step
    
    return max_id - 1


async def scrape_thumbnail_urls(category: str, start_id: int, end_id: int, tick_progress: Callable | None = None) -> AsyncGenerator[str]:
    prev_urls = set()

    tasks = []
    ids = []
    for entry_id in range(start_id, end_id + 1):
        tasks.append(wrap_coroutine_with_value(fetch_and_parse_entry(category, entry_id), entry_id))
        ids.append(entry_id)
    
    async for task in asyncio.as_completed(tasks):
        try:
            soup, entry_id = await task
            tick_progress()
            if soup is None:
                logging.info(f"{category}#{entry_id}: Page not found, skipping")
                continue

            urls = extract_image_urls(soup)
            n_urls = len(urls)

            if n_urls == 0:
                logging.info(f"{category}#{entry_id}: No thumbnails, skipping")
                continue

            logging.info(f"{category}#{entry_id}: {len(urls)} thumbnail{'s' if n_urls > 1 else ''}")

            for url in extract_image_urls(soup):
                if url not in prev_urls:
                    logging.info(f"{category}#{entry_id}: New image: {url}")
                    prev_urls.add(url)
                    yield url
                else:
                    logging.info(f"{category}#{entry_id}: Skipping previous image: {url}")

        except HTTPStatusError as err:
            tick_progress()
            logging.info(f"{category}#{entry_id}: Scrape error, status code {err.response.status_code}")


async def does_entry_exist(category: str, entry_id: int) -> bool:
    key = (category, entry_id)
    try:
        return PAGE_VALIDITY_CACHE[key]
    except KeyError:
        response = await fetch_entry(category, entry_id)
        exists = response.is_success
        PAGE_VALIDITY_CACHE[key] = exists
        return exists


async def fetch_and_parse_entry(category: str, entry_id: int) -> BeautifulSoup | None:
    url = BASE_URL + category + '.aspx'

    response = await fetch_entry(category, entry_id)

    if response.is_error:
        return None

    return BeautifulSoup(response.content, "html.parser")


async def fetch_entry(category: str, entry_id: int, backoff_start: int = 1, backoff_max: int = 16) -> Response:
    url = BASE_URL + category + '.aspx'

    success = False
    backoff = backoff_start
    while not success:
        logging.info(f"{category}#{entry_id}: Fetching entry")
        try:
            async with SEMAPHORE:
                response = await CLIENT.get(url, params={'ID': str(entry_id)})
            success = True
        except ReadTimeout:
            logging.error(f"{category}#{entry_id}: Request timed out, waiting {backoff} s before retry")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)
    
    return response


def extract_image_urls(soup: BeautifulSoup) -> list[str]:
    urls = []
    for element in soup.find_all("img", class_="thumbnail"):
        urls.append(make_image_url_absolute(element.get("src")))
    return urls


def make_image_url_absolute(url: str) -> str:
    return BASE_URL + '/'.join(url.split('\\'))


async def wrap_coroutine_with_value(coro, value):
    return (await coro, value)

class ScrapeError(Exception):
    def __init__(self, *args, response=None):
        super().__init__(*args)
        self.response = response


def run():
    asyncio.run(main())

if __name__ == "__main__":
    run()
