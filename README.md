# nethys-archivist

Paizo recently terminated their partnership with the [Archives of Nethys](https://2e.aonprd.com),
which among other things means the Archives will no longer be allowed to use the vast majority of art from the Pathfinder sourcebooks.
This made me sad because I love looking at the art for monsters and ancestries and such,
so I made this program to scrape it before it all gets removed.
I can't distribute the scraped art, of course, but I can share this code.

**Please don't misuse this!**
It follows good practices for scraping (limiting concurrent connections, exponential backoff on retries),
but it does by necessity make a lot of requests to AoN.
If too many people use it simultaneously or repeatedly, it could seriously tax the servers.
I don't think anyone in the Pathfinder community wants that.
If you want to use it, please only run it once to create a personal archive for yourself.

## Usage

Have [`uv`](https://docs.astral.sh/uv/) installed, clone this repository, then run:
```shell
uv sync
uv run nethys-archivist.py
```
to download all the art! You can also specify certain categories to download if you don't want all of it; just make sure the categories match the names of the pages on AoN. E.g. to download monster images, the AoN category URL is `2e.aonprd.com/Monsters.aspx`, so you'd do `uv run nethys-archivist monsters`. (It's not case sensitive.)

Alternatively, if you don't want to use uv, just make sure your Python 3.14 environment has these packages at these versions:
- `httpx>=0.28.1`
- `beautifulsoup4>=4.15.0`
- `aiofiles>=25.1.0`
- `rich>=15.0.0`

Then run `python nethys-archivist.py`.

The art will be placed in `download/` organized by category.
Most of them have self-explanatory names,
so you can probably just search by filename to find what you're looking for.

## Note

For some reason, the Archetypes pages can be really slow to respond compared to other categories.
Be patient, it'll work.