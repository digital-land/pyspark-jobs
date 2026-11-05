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

# Links on the page that look like a data file, offered as the likely
# replacement when the endpoint is not found. Capped so a page listing every
# file the council has ever published does not make an enormous task.
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


class _HrefCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value.strip())


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


def fetch_page(documentation_url):
    """(final_url, hrefs, error) for a documentation page.

    error is a short fixed category rather than the exception text: it ends up
    in the task details, which feed the reference hash, so it must not carry
    anything that changes from one night to the next.
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
        return documentation_url, [], "timeout"
    except requests.exceptions.HTTPError as error:
        return documentation_url, [], f"http-{error.response.status_code}"
    except requests.exceptions.RequestException:
        return documentation_url, [], "connection-error"

    collector = _HrefCollector()
    try:
        collector.feed(response.text)
    except Exception:
        return response.url, [], "parse-error"
    return response.url, collector.hrefs, None


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


def _candidate_links(pages):
    links = set()
    for page_url, hrefs, _ in pages:
        for href in hrefs:
            try:
                absolute = urljoin(page_url, href)
            except ValueError:
                continue
            if urlparse(absolute).path.lower().endswith(_DATA_FILE_SUFFIXES):
                links.add(absolute)
    return sorted(links)[:_MAX_CANDIDATE_LINKS]


def _file_name(url):
    return unquote(urlparse(url).path).rstrip("/").rsplit("/", 1)[-1].lower()


def _likely_replacement(endpoint_url, candidate_links):
    """The first candidate with the endpoint's file name: the same file
    republished at a new URL, as a CMS does on every upload. "" if none."""
    name = _file_name(endpoint_url)
    if not name:
        return ""
    for link in candidate_links:
        if _file_name(link) == name:
            return link
    return ""


def check_endpoint(endpoint_url, documentation_urls, pages):
    """The outcome of checking one endpoint against its documentation pages.

    pages maps each documentation URL to fetch_page's result. The endpoint
    counts as linked if ANY of its pages links to it, as an endpoint can sit
    behind several sources.
    """
    fetched = [pages[url] for url in documentation_urls]
    for page_url, hrefs, _ in fetched:
        for href in hrefs:
            if hrefs_match(href, endpoint_url, page_url):
                return {"result": FOUND, "matched_href": href}

    errors = sorted({error for _, _, error in fetched if error})
    if all(error for _, _, error in fetched):
        gone = all(error in _GONE_ERRORS for _, _, error in fetched)
        return {
            "result": DOCUMENTATION_URL_GONE if gone else UNREACHABLE,
            "fetch_errors": errors,
        }
    candidate_links = _candidate_links(fetched)
    return {
        "result": NOT_FOUND,
        "fetch_errors": errors,
        "redirects": _redirects(documentation_urls, pages),
        "candidate_links": candidate_links,
        "likely_replacement": _likely_replacement(endpoint_url, candidate_links),
    }


def check_endpoints(endpoints, max_workers=_MAX_WORKERS):
    """Check each endpoint against its documentation pages.

    endpoints: iterable of dicts with endpoint, endpoint_url and
    documentation_urls (a list). Each documentation page is fetched once,
    however many endpoints it serves.

    Returns one dict per endpoint: the input keys plus check_endpoint's.
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
        }
        for endpoint in endpoints
    ]
    counts = {}
    for result in results:
        counts[result["result"]] = counts.get(result["result"], 0) + 1
    logger.info(f"check_endpoints: {counts}")
    return results
