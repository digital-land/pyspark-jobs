"""Check that an endpoint URL is linked from its documentation page.

The matching follows config-manager's doc_crawler (check_endpoint_in_doc), which
the Manage service uses for the same check, so an endpoint Manage shows as
linked is not raised as a task here. Where it differs:

- links are resolved against the page's final URL after redirects, rather than
  the URL we asked for
- URLs are compared percent-decoded, since endpoint.csv holds some URLs with
  literal spaces where the page's href has %20
- an ArcGIS or WFS endpoint is a query we build against a service, which a page
  links to as the service (".../FeatureServer") rather than as our query
  (".../FeatureServer/0/query?where=1%3D1&f=geojson"), so for those a link to the
  same service counts

The same fetch also checks whether the page says the data is published under the
Open Government Licence (mentions_ogl), so each page is requested once for both
checks.

Plain Python on the driver, not Spark: the work is a few thousand HTTP requests,
which a thread pool does well and a Spark job has no advantage at.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests

from jobs.config.documentation_url_task import (
    DOCUMENTATION_URL_GONE,
    NOT_FOUND,
    UNREACHABLE,
)
from jobs.config.licence_task import OGL_FOUND, OGL_NOT_FOUND

logger = logging.getLogger(__name__)

FOUND = "found"

_TIMEOUT_SECONDS = 10
# Low because these requests share the collector's IP addresses: a council that
# blocks them for sending too much traffic would block the collection too.
_MAX_WORKERS = 4
# The collector's user agent (digital-land-python collect.py), which LPAs are told
# to allow when their site blocks us, so this check is let through too.
_USER_AGENT = "MHCLG Planning Data Collector"

# A page that no longer exists, as against one we could not read.
_GONE_ERRORS = {"http-404", "http-410"}

# Links on the page that look like a data file, from which the likely
# replacement is chosen when the endpoint is not found. The candidate_links
# stored in the task are capped so a page listing every file the council has
# ever published does not make an enormous task; the choice uses them all.
_DATA_FILE_SUFFIXES = (
    ".csv",
    ".xlsx",
    ".xls",
    ".json",
    ".geojson",
    ".gml",
    ".kml",
    ".zip",
)
_MAX_CANDIDATE_LINKS = 20

_ARCGIS_SERVICE = re.compile(r"^(.*?/(?:featureserver|mapserver|imageserver))(?:/|$)")

# How a page says its data is under the Open Government Licence: by name ("Open
# Government Licence v3.0", or the US spelling), by its abbreviation ("OGL v3"),
# or by linking to the licence on the National Archives site, as the OGL logo
# does. Read from the whole page, footer included, as many councils state it once
# in a site-wide footer. Any version counts.
_OGL_NAME = re.compile(r"open\s+government\s+licen[cs]e", re.IGNORECASE)
_OGL_ABBREVIATION = re.compile(r"\bOGL\b")
_OGL_LINK = "/doc/open-government-licence"

# Text in these is code, not something a reader sees.
_NOT_TEXT = {"script", "style", "template"}


# Outlook rewrites the links in an email to go through its scanner, so a link
# copied from an email onto a website goes to Microsoft with the real URL inside.
_SAFE_LINKS_HOST = re.compile(r"(^|\.)safelinks\.protection\.outlook\.com$")


def _unwrapped(href):
    """The real URL inside an Outlook Safe Links href, or href itself."""
    try:
        parsed = urlparse(href)
    except ValueError:
        return href
    if not _SAFE_LINKS_HOST.search((parsed.hostname or "").lower()):
        return href
    url = parse_qs(parsed.query).get("url")
    return url[0] if url else href


class _PageParser(HTMLParser):
    """Collects a page's anchor hrefs and its visible text."""

    def __init__(self):
        super().__init__()
        self.hrefs = []
        self.text = []
        self._skipping = 0

    def handle_starttag(self, tag, attrs):
        if tag in _NOT_TEXT:
            self._skipping += 1
        for name, value in attrs:
            if not value:
                continue
            if tag == "a" and name == "href":
                self.hrefs.append(_unwrapped(value.strip()))
            # an OGL logo's alt text names the licence
            elif tag == "img" and name == "alt":
                self.text.append(value)

    def handle_endtag(self, tag):
        if tag in _NOT_TEXT and self._skipping:
            self._skipping -= 1

    def handle_data(self, data):
        if not self._skipping:
            self.text.append(data)


def _without_scheme_or_query(url):
    """Lowercased host and path, so http/https and query strings don't matter."""
    parsed = urlparse(unquote(url).lower())
    return f"{parsed.netloc}{parsed.path}".rstrip("/")


def service_root(endpoint_url):
    """The service an ArcGIS or WFS endpoint queries, or None for anything else.

    ArcGIS: everything up to the FeatureServer/MapServer/ImageServer segment.
    WFS: the service URL without its query string.
    """
    try:
        bare = _without_scheme_or_query(endpoint_url)
        query = {
            k.lower(): v for k, v in parse_qs(urlparse(endpoint_url).query).items()
        }
    except ValueError:
        return None
    arcgis = _ARCGIS_SERVICE.match(bare)
    if arcgis:
        return arcgis.group(1)
    services = [value.lower() for value in query.get("service", [])]
    if "wfs" in services or "/wfs" in bare or bare.endswith("/ows"):
        return bare
    return None


def hrefs_match(href, endpoint_url, page_url):
    """True if href points at endpoint_url, by any of doc_crawler's strategies
    (exact, resolved against the page, path-only, or the path of an absolute
    href), or for an ArcGIS or WFS endpoint, if href points at the same service."""
    href = unquote(href)
    endpoint_url = unquote(endpoint_url)
    endpoint_path = urlparse(endpoint_url).path

    if href == endpoint_url:
        return True

    try:
        resolved = urljoin(page_url, href)
    except ValueError:
        return False
    if unquote(resolved) == endpoint_url:
        return True

    if href == endpoint_path:
        return True

    href_path = urlparse(href).path
    if href_path and href_path == endpoint_path:
        return True

    root = service_root(endpoint_url)
    if root:
        linked = _without_scheme_or_query(resolved)
        return linked == root or linked.startswith(root + "/")
    return False


def mentions_ogl(text, hrefs):
    """True if a page's text or links say it is under the Open Government Licence."""
    text = " ".join(text.split())
    return bool(
        _OGL_NAME.search(text)
        or _OGL_ABBREVIATION.search(text)
        or any(_OGL_LINK in unquote(href).lower() for href in hrefs)
    )


def fetch_page(documentation_url):
    """(final_url, hrefs, error, ogl) for a documentation page.

    error is a short fixed category rather than the exception text: it ends up
    in the task details, which feed the reference hash, so it must not carry
    anything that changes from one night to the next. ogl is whether the page
    mentions the Open Government Licence, False when it could not be read.
    """
    try:
        response = requests.get(
            documentation_url,
            timeout=_TIMEOUT_SECONDS,
            allow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )
        response.raise_for_status()
    except requests.exceptions.Timeout:
        return documentation_url, [], "timeout", False
    except requests.exceptions.HTTPError as error:
        return documentation_url, [], f"http-{error.response.status_code}", False
    except requests.exceptions.RequestException:
        return documentation_url, [], "connection-error", False

    parser = _PageParser()
    try:
        parser.feed(response.text)
    except Exception:
        return response.url, [], "parse-error", False
    ogl = mentions_ogl(" ".join(parser.text), parser.hrefs)
    return response.url, parser.hrefs, None, ogl


def _redirects(documentation_urls, pages):
    """{documentation URL: where it now goes} for pages that moved somewhere else.

    A change of scheme, a www. prefix or a trailing slash is not a move.
    """

    def page(url):
        bare = _without_scheme_or_query(url)
        return bare[4:] if bare.startswith("www.") else bare

    return {
        url: pages[url][0]
        for url in documentation_urls
        if not pages[url][2] and page(pages[url][0]) != page(url)
    }


def _data_links(pages):
    """Every link on the pages that looks like a data file, sorted. The likely
    replacement is chosen from all of them; only the first _MAX_CANDIDATE_LINKS
    go into the task as candidate_links. Sorted order keeps the oldest of a
    page's yearly files first, so choosing from the capped list would miss the
    latest."""
    links = set()
    for page_url, hrefs, _, _ in pages:
        for href in hrefs:
            try:
                absolute = urljoin(page_url, href)
            except ValueError:
                continue
            if urlparse(absolute).path.lower().endswith(_DATA_FILE_SUFFIXES):
                links.add(absolute)
    return sorted(links)


def _file_name(url):
    return unquote(urlparse(url).path).rstrip("/").rsplit("/", 1)[-1].lower()


# Drupal, which many council sites run on, saves a file uploaded under a name it
# already has as name_0.csv, then name_1.csv, rather than replacing it.
_DRUPAL_RENAME = re.compile(r"^(.+)_\d{1,2}(\.[^.]+)$")


def _drupal_renamed(name, other):
    """True if one name is the other with Drupal's _0, _1, ... added. Not two
    numbered names (part_1.csv and part_2.csv), which are usually different
    files, nor a year or date (register_2025.csv)."""
    for renamed, original in ((name, other), (other, name)):
        match = _DRUPAL_RENAME.match(renamed)
        if match and match.group(1) + match.group(2) == original:
            return True
    return False


def _likely_replacement(endpoint_url, candidate_links):
    """The first candidate with the endpoint's file name: the same file
    republished at a new URL, as a CMS does on every upload. Failing that, the
    first with the name as Drupal renames it on a new upload. "" if none."""
    name = _file_name(endpoint_url)
    if not name:
        return ""
    for link in candidate_links:
        if _file_name(link) == name:
            return link
    for link in candidate_links:
        if _drupal_renamed(_file_name(link), name):
            return link
    return ""


def _singular(word):
    return word[:-1] if len(word) > 3 and word.endswith("s") else word


def _words(url):
    """The words in a link's file name, singular, so that
    BrownfieldLandRegister2024.csv, brownfield_land_registers_2024.csv and
    Brownfield Land Register 2024.csv all give brownfield, land, register, 2024,
    and local-plan-timetablev3.csv gives local, plan, timetable, v, 3."""
    name = unquote(urlparse(url).path).rstrip("/").rsplit("/", 1)[-1]
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    # a version run on to a word: timetablev3
    name = re.sub(r"(?<=[A-Za-z]{2})(?=[vV]\d)", " ", name)
    name = re.sub(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])", " ", name)
    return {_singular(word) for word in re.findall(r"[a-z0-9]+", name.lower())}


def _named_datasets(link, dataset_names):
    """The datasets whose name, or another name for it, is in the link's file
    name, keeping only the longest name found: a local plan timetable file is
    plan-timetable's, not local-plan's."""
    words = _words(link)
    named, longest = set(), 0
    for dataset, names in dataset_names.items():
        for name in names:
            parts = {_singular(word) for word in name.split("-")}
            if parts <= words:
                if len(parts) > longest:
                    named, longest = {dataset}, len(parts)
                elif len(parts) == longest:
                    named.add(dataset)
    return named


def _file_type(url):
    name = _file_name(url)
    return name.rsplit(".", 1)[-1] if "." in name else ""


_MONTHS = {
    month: number
    for number, names in enumerate(
        (
            ("jan", "january"),
            ("feb", "february"),
            ("mar", "march"),
            ("apr", "april"),
            ("may",),
            ("jun", "june"),
            ("jul", "july"),
            ("aug", "august"),
            ("sep", "sept", "september"),
            ("oct", "october"),
            ("nov", "november"),
            ("dec", "december"),
        ),
        start=1,
    )
    for month in names
}
# 2025, 202511, 2025-11, 2025-11-03, 20251103
_YEAR_FIRST = re.compile(
    r"(?<!\d)(20[0-4]\d)"
    r"(?:[-_ .]?(0[1-9]|1[0-2])(?:[-_ .]?(0[1-9]|[12]\d|3[01]))?)?(?!\d)"
)
# 03-11-25, 3.11.25
_DAY_FIRST = re.compile(
    r"(?<!\d)(0?[1-9]|[12]\d|3[01])[-_.](0?[1-9]|1[0-2])[-_.](\d{2})(?!\d)"
)


def _date_in_name(url):
    """The latest date in a link's file name as (year, month, day), with 0 for a
    part not given, or None if the name has no year. Dates written as numbers
    (2025-11-03, 202511, 03.11.25) and as words (November 2025, Dec2025)."""
    name = _file_name(url)
    dates = [
        (int(year), int(month or 0), int(day or 0))
        for year, month, day in _YEAR_FIRST.findall(name)
    ]
    dates += [
        (2000 + int(year), int(month), int(day))
        for day, month, year in _DAY_FIRST.findall(name)
    ]
    words = re.findall(r"[a-z]+|\d+", name)
    for i, word in enumerate(words):
        if word in _MONTHS:
            for year in words[i + 1 : i + 3]:
                if re.fullmatch(r"20[0-4]\d", year):
                    dates.append((int(year), _MONTHS[word], 0))
    return max(dates) if dates else None


def _latest(links):
    """The links whose file name has the latest date, when every one has a date:
    a page often keeps each year's register. All of them otherwise."""
    dates = [_date_in_name(link) for link in links]
    if not all(dates):
        return links
    latest = max(dates)
    return [link for link, date in zip(links, dates) if date == latest]


def _without_type(url):
    return re.sub(r"\.[^.]+$", "", _file_name(url))


def likely_replacement_for_dataset(
    dataset, endpoint_url, candidate_links, dataset_names
):
    """The candidate named for the dataset, for when none has the endpoint's
    file name, as when a council dates each upload. Where several are named for
    it, narrowed down in turn to:

    - those with the latest date in their name, as a page often keeps each
      year's file
    - those of the endpoint's file type, as a council often publishes the same
      data as CSV and XLSX
    - the CSV, where what is left is one file in several formats

    "" unless that leaves exactly one, as choosing between them needs a person.

    dataset_names maps each dataset we collect to the names its files are
    published under, its own first. Every dataset is needed, not just this one,
    to tell whose a file is when one name contains another.
    """
    if not dataset:
        return ""
    named = [
        link
        for link in candidate_links
        if dataset in _named_datasets(link, dataset_names)
    ]
    if len(named) > 1:
        named = _latest(named)
    if len(named) > 1:
        same_type = [
            link for link in named if _file_type(link) == _file_type(endpoint_url)
        ]
        named = same_type or named
    if len(named) > 1 and len({_without_type(link) for link in named}) == 1:
        csvs = [link for link in named if _file_type(link) == "csv"]
        named = csvs or named
    return named[0] if len(named) == 1 else ""


def check_endpoint(endpoint_url, documentation_urls, pages):
    """The outcome of checking one endpoint against its documentation pages.

    pages maps each documentation URL to fetch_page's result. The endpoint
    counts as linked if ANY of its pages links to it, as an endpoint can sit
    behind several sources.
    """
    fetched = [pages[url] for url in documentation_urls]
    for page_url, hrefs, _, _ in fetched:
        for href in hrefs:
            if hrefs_match(href, endpoint_url, page_url):
                return {"result": FOUND, "matched_href": href}

    errors = sorted({error for _, _, error, _ in fetched if error})
    if all(error for _, _, error, _ in fetched):
        gone = all(error in _GONE_ERRORS for _, _, error, _ in fetched)
        return {
            "result": DOCUMENTATION_URL_GONE if gone else UNREACHABLE,
            "fetch_errors": errors,
        }
    data_links = _data_links(fetched)
    return {
        "result": NOT_FOUND,
        "fetch_errors": errors,
        "redirects": _redirects(documentation_urls, pages),
        "candidate_links": data_links[:_MAX_CANDIDATE_LINKS],
        "likely_replacement": _likely_replacement(endpoint_url, data_links),
        # every data link, for the pipeline to choose a likely replacement named
        # for each dataset from; not stored in the task
        "data_links": data_links,
    }


def check_licence(documentation_urls, pages):
    """Whether an endpoint's documentation pages say the data is under the OGL.

    pages maps each documentation URL to fetch_page's result. One page saying so
    is enough. Returns None when no page could be read, as nothing is known about
    the licence then.
    """
    fetched = [pages[url] for url in documentation_urls]
    if any(ogl for _, _, _, ogl in fetched):
        return {"result": OGL_FOUND}
    if all(error for _, _, error, _ in fetched):
        return None
    return {
        "result": OGL_NOT_FOUND,
        "fetch_errors": sorted({error for _, _, error, _ in fetched if error}),
    }


def check_endpoints(endpoints, max_workers=_MAX_WORKERS):
    """Check each endpoint against its documentation pages.

    endpoints: iterable of dicts with endpoint, endpoint_url and
    documentation_urls (a list). Each documentation page is fetched once,
    however many endpoints it serves.

    Returns one dict per endpoint: the input keys plus check_endpoint's, and
    "licence" holding check_licence's result.
    """
    endpoints = list(endpoints)
    documentation_urls = sorted(
        {url for endpoint in endpoints for url in endpoint["documentation_urls"]}
    )
    logger.info(
        f"check_endpoints: fetching {len(documentation_urls)} documentation pages "
        f"for {len(endpoints)} endpoints"
    )
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        pages = dict(zip(documentation_urls, pool.map(fetch_page, documentation_urls)))

    results = [
        {
            **endpoint,
            **check_endpoint(
                endpoint["endpoint_url"], endpoint["documentation_urls"], pages
            ),
            "licence": check_licence(endpoint["documentation_urls"], pages),
        }
        for endpoint in endpoints
    ]
    counts = {}
    for result in results:
        counts[result["result"]] = counts.get(result["result"], 0) + 1
    logger.info(f"check_endpoints: {counts}")
    licence_counts = {}
    for result in results:
        key = result["licence"]["result"] if result["licence"] else "unread"
        licence_counts[key] = licence_counts.get(key, 0) + 1
    logger.info(f"check_endpoints: licence {licence_counts}")
    return results
