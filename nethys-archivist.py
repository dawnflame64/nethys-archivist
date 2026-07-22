import asyncio
from collections.abc import AsyncGenerator, Callable
import logging
from pathlib import Path
import os
from argparse import ArgumentParser
from functools import partial

from httpx import AsyncClient, HTTPStatusError, Limits, Response, ReadTimeout
from bs4 import BeautifulSoup
import aiofiles
from rich.progress import Progress, MofNCompleteColumn

BASE_URL = "https://2e.aonprd.com/"
BASE_DOWNLOAD_DIR = Path("download")
IMAGES_DIR = BASE_DOWNLOAD_DIR/"Images"

ALL_CATEGORIES_WITH_IMAGES = {
    'Ancestries': (200, 64),
    'Classes': (100, 32),
    'Archetypes': (500, 64),
    'Equipment': (10000, 256),
    'Armor': (100, 32),
    'Shields': (100, 32),
    'Weapons': (500, 64),
    'NPCs': (2000, 64),
    'Monsters': (10000, 128),
    'Deities': (1000, 128),
}

ALL_CATEGORIES = ALL_CATEGORIES_WITH_IMAGES | {
    'Backgrounds': (1000, 64),
    'Skills': (100, 32),
    'Companions': (500, 64),
    'Familiars': (500, 64),
    'Feats': (10000, 256),
    'Curses': (500, 64),
    'Diseases': (500, 64),
    'ItemCurses': (500, 64),
    'Hazards': (1000, 128),
    'Domains': (500, 64),
    'Planes': (100, 32),
    'Spells': (10000, 256),
    'Rituals': (500, 64),
    'MythicCallings': (50, 16)
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
SEMAPHORE_REQ = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

SEMAPHORE_FILES = asyncio.Semaphore(510) # should be lower than any OS limit

PAGE_VALIDITY_CACHE = {}

logging.basicConfig(filename="archivist.log", filemode='w', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.CRITICAL)

NOOP = lambda *_, **__: None

async def main():
    parser = ArgumentParser(prog="nethys-archivist")
    parser.add_argument('-c', '--count', action='store_true', help="Instead of downloading, only display the number of pages that will be scraped and checked for images in the specified categories.")
    parser.add_argument('--no-save-pages', action='store_true', help="Do not download the entries themselves, only images.")
    parser.add_argument('categories', nargs='*', help="The categories of entry to download images from (Ancestries, Classes, Equipment...) If not specified, every category will be downloaded.")

    args = parser.parse_args()

    if args.categories:
        categories = [cat.title() for cat in args.categories]
    else:
        if args.no_save_pages:
            categories = list(ALL_CATEGORIES_WITH_IMAGES.keys())
        else:
            categories = list(ALL_CATEGORIES.keys())

    if args.count:
        logging.info("Running in category count mode")
        for category in categories:
            min_id, max_id = await get_category_id_bounds(category)
            print(f"{category}: ~{max_id - min_id + 1}")
    else:
        logging.info("Running in download mode")
        await download_multiple_categories(categories, download_pages=not args.no_save_pages, parallel_categories=True)
    
    await CLIENT.aclose()


async def download_multiple_categories(categories: list[str], overwrite_if_exists: bool = False, download_pages: bool = True, parallel_categories: bool = False):
    if not os.path.exists(BASE_DOWNLOAD_DIR):
        logging.info(f"Creating directory {BASE_DOWNLOAD_DIR}")
        os.mkdir(BASE_DOWNLOAD_DIR)

    if not os.path.exists(IMAGES_DIR):
        logging.info(f"Creating directory {IMAGES_DIR}")
        os.mkdir(IMAGES_DIR)

    max_category_name_len = max(*(len(category) for category in categories))

    with Progress(*Progress.get_default_columns(), MofNCompleteColumn()) as progress:
        downloads = []
        for category in categories:
            has_images = category in ALL_CATEGORIES_WITH_IMAGES
            if not has_images and not download_pages:
                logging.info(f"Skipping category {category} since it has no images and --no-save-pages is set")
                continue

            extra_spaces = ' ' * (max_category_name_len - len(category))

            task_scan = progress.add_task(f"[magenta][{category}]{extra_spaces} Scanning entries...", total=None)

            if download_pages:
                scrape_text = "Downloading entries..."
            else:
                scrape_text = "Scraping image URLs..."

            task_scrape = progress.add_task(f"[yellow][{category}]{extra_spaces} {scrape_text}", visible=False, total=0)

            desc_when_complete = f"[light_green][{category}]{extra_spaces} Finished!"

            if has_images:
                task_download = progress.add_task(f"[light_green][{category}]{extra_spaces} Downloading images...", visible=False, total=0)
                mark_complete = partial(progress.update, task_download, description=desc_when_complete)
            else:
                task_download = None
                mark_complete = partial(progress.update, task_scrape, description=desc_when_complete)

            task = download_category(
                category,
                overwrite_if_exists=overwrite_if_exists,
                download_pages=download_pages,
                no_thumbnails=not has_images,
                update_scan=partial(progress.update, task_scan),
                update_scrape=partial(progress.update, task_scrape),
                update_download=partial(progress.update, task_download),
                mark_complete=mark_complete
            )

            if parallel_categories:
                downloads.append(task)
            else:
                await task

        if downloads:
            await asyncio.gather(*downloads)


async def download_category(
    category: str,
    overwrite_if_exists: bool = False,
    download_pages: bool = True,
    no_thumbnails: bool = False,
    update_scan: Callable | None = None,
    update_scrape: Callable | None = None,
    update_download: Callable | None = None,
    mark_complete: Callable | None = None
):
    update_scan = update_scan or NOOP
    update_scrape = update_scrape or NOOP
    update_download = update_download or NOOP
    mark_complete = mark_complete or NOOP

    if not no_thumbnails:
        category_dir = IMAGES_DIR/category
        if not os.path.exists(category_dir):
            logging.info(f"Creating directory {category_dir}")
            os.mkdir(category_dir)

    min_id, max_id = await get_category_id_bounds(category)

    update_scan(visible=False)
    update_scrape(visible=True, total=max_id - min_id + 1, completed=0)

    if not no_thumbnails:
        await download_thumbnails_in_range(
            category_dir,
            category,
            min_id,
            max_id,
            overwrite_if_exists=overwrite_if_exists,
            download_pages=download_pages,
            update_scrape=update_scrape,
            update_download=update_download
        )
    else:
        await download_pages_no_thumbnails(
            category,
            min_id,
            max_id,
            tick_progress=partial(update_scrape, advance=1)
        )

    mark_complete()



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
    download_pages: bool = True,
    update_scrape: Callable | None = None,
    update_download: Callable | None = None
):
    update_scrape = update_scrape or NOOP
    update_download = update_download or NOOP

    base_path = Path(base_path)
    
    downloads = []
    n_downloads = 0
    async for url in scrape_thumbnail_urls(
        category,
        start_id,
        end_id,
        download_pages=download_pages,
        tick_progress=partial(update_scrape, advance=1)
    ):
        filename = url.split('/')[-1]
        path = base_path/filename

        downloads.append(download_image(url, path, overwrite_if_exists=overwrite_if_exists, tick_progress=partial(update_download, advance=1)))
        n_downloads += 1
        update_download(visible=True, total=n_downloads)
    
    update_scrape(visible=False)
    
    await asyncio.gather(*downloads)


async def download_image(url: str, path: Path | str, overwrite_if_exists: bool = False, tick_progress: Callable | None = None):
    tick_progress = tick_progress or NOOP

    path = Path(path)

    if os.path.exists(path):
        if overwrite_if_exists:
            logging.info(f"Overwriting existing file at {path}")
        else:
            logging.info(f"Skipping existing file at {path}")
            tick_progress()
            return
    
    async with SEMAPHORE_REQ:
        response = await CLIENT.get(url)

    if response.is_error:
        logging.error(f"Error downloading image at {url} ({response.status_code})")
        return

    async with SEMAPHORE_FILES:
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

    original_max_id = max_id
    
    step = initial_step
    while step > 0 and max_id > 0:
        logging.debug(f"Search {category}: min={min_id}, max={max_id}, step={step}")
        to_test = max_id - step
        if max_id - step >= min_id and await does_entry_exist(category, to_test):
            logging.debug(f"Search {category}: entry exists, halving step")
            step //= 2
        else:
            logging.debug(f"Search {category}: Entry does not exist, continuing")
            max_id = to_test

    if max_id <= 0:
        logging.debug(f"Search {category}: trying again with a finer step")
        return await search_max_id(category, min_id=min_id, max_id=original_max_id, initial_step=initial_step // 2)
    else:
        return max_id - 1


async def scrape_thumbnail_urls(category: str, start_id: int, end_id: int, download_pages: bool = True, tick_progress: Callable | None = None) -> AsyncGenerator[str]:
    tick_progress = tick_progress or NOOP
    
    prev_urls = set()

    tasks = []
    for entry_id in range(start_id, end_id + 1):
        tasks.append(wrap_coroutine_with_value(fetch_and_parse_entry(category, entry_id, save=download_pages), entry_id))
    
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


async def download_pages_no_thumbnails(category: str, start_id: int, end_id: int, tick_progress: Callable | None = None):
    tick_progress = tick_progress or NOOP

    tasks = []
    for entry_id in range(start_id, end_id + 1):
        tasks.append(wrap_coroutine_with_value(fetch_entry(category, entry_id, save=True), entry_id))

    async for task in asyncio.as_completed(tasks):
        _, entry_id = await task
        tick_progress()


async def does_entry_exist(category: str, entry_id: int) -> bool:
    key = (category, entry_id)
    try:
        return PAGE_VALIDITY_CACHE[key]
    except KeyError:
        response = await fetch_entry(category, entry_id)
        exists = response.is_success
        PAGE_VALIDITY_CACHE[key] = exists
        return exists


async def fetch_and_parse_entry(category: str, entry_id: int, save: bool = True) -> BeautifulSoup | None:
    url = BASE_URL + category + '.aspx'

    response = await fetch_entry(category, entry_id, save=True)

    if response.is_error:
        return None

    return BeautifulSoup(response.content, "html.parser")


async def fetch_entry(category: str, entry_id: int, save: bool = True, backoff_start: int = 1, backoff_max: int = 16) -> Response:
    url = BASE_URL + category + '.aspx'

    save_path = get_entry_save_path(category, entry_id)

    if os.path.exists(save_path):
        logging.info(f"{category}#{entry_id}: Entry already downloaded at {save_path}")
        async with SEMAPHORE_FILES:
            async with aiofiles.open(save_path, 'rb') as fp:
                content = await fp.read()
        return Response(200, content=content)

    success = False
    backoff = backoff_start
    while not success:
        logging.info(f"{category}#{entry_id}: Fetching entry")
        try:
            async with SEMAPHORE_REQ:
                response = await CLIENT.get(url, params={'ID': str(entry_id)})
            success = True
        except ReadTimeout:
            logging.error(f"{category}#{entry_id}: Request timed out, waiting {backoff} s before retry")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)
    
    if response.is_success:
        async with SEMAPHORE_FILES:
            async with aiofiles.open(save_path, 'wb') as fp:
                await fp.write(response.content)
        logging.info(f"{category}#{entry_id}: Saved entry to {save_path}")
    
    return response


def get_entry_save_path(category: str, entry_id: int) -> Path:
    return BASE_DOWNLOAD_DIR/f"{category}_{entry_id:05}.html"


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
