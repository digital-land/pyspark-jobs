"""
Unit tests for the documentation URL check. No Spark and no network: requests
is replaced so each test serves its own page.
"""

import pytest
import requests

from jobs.config.documentation_url_task import (
    DOCUMENTATION_URL_GONE,
    NOT_FOUND,
    UNREACHABLE,
)
from jobs.utils import documentation_url
from jobs.utils.documentation_url import (
    FOUND,
    check_endpoint,
    check_endpoints,
    fetch_page,
    hrefs_match,
    service_root,
)

PAGE = "https://council.gov.uk/planning/local-plan"
ENDPOINT = "https://council.gov.uk/downloads/file/3149/timetable.csv"


class _Response:
    def __init__(self, text="", status_code=200, url=PAGE):
        self.text = text
        self.status_code = status_code
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)


def _serve(mocker, pages):
    """Serve pages[url], which is an HTML string, a _Response, or an exception."""

    def get(url, **_):
        page = pages[url]
        if isinstance(page, Exception):
            raise page
        if isinstance(page, _Response):
            return page
        return _Response(page, url=url)

    return mocker.patch.object(documentation_url.requests, "get", side_effect=get)


# -- hrefs_match --------------------------------------------------------------


@pytest.mark.parametrize(
    "href",
    [
        ENDPOINT,
        "/downloads/file/3149/timetable.csv",
        "../downloads/file/3149/timetable.csv",
        "http://www.council.gov.uk/downloads/file/3149/timetable.csv",
    ],
)
def test_hrefs_match_the_strategies_manage_uses(href):
    assert hrefs_match(href, ENDPOINT, PAGE)


def test_hrefs_match_compares_percent_decoded():
    """endpoint.csv holds some URLs with literal spaces where the href has %20."""
    assert hrefs_match(
        "https://council.gov.uk/files/plan%20timetable.csv",
        "https://council.gov.uk/files/plan timetable.csv",
        PAGE,
    )


ARCGIS = "https://services.arcgis.com/abc/arcgis/rest/services/A4D/FeatureServer"
WFS = "https://maps.council.gov.uk/geoserver/ows"


@pytest.mark.parametrize(
    "endpoint_url, root",
    [
        (
            f"{ARCGIS}/0/query?where=1%3D1&f=geojson",
            "services.arcgis.com/abc/arcgis/rest/services/a4d/featureserver",
        ),
        (
            f"{ARCGIS}/0/",
            "services.arcgis.com/abc/arcgis/rest/services/a4d/featureserver",
        ),
        (
            "https://gis.council.gov.uk/arcgis/rest/services/Planning/MapServer/3",
            "gis.council.gov.uk/arcgis/rest/services/planning/mapserver",
        ),
        (
            f"{WFS}?service=WFS&request=GetFeature&typeName=a4",
            "maps.council.gov.uk/geoserver/ows",
        ),
        (
            "https://medwaymaps.medway.gov.uk/WFS/wfs.exe?service=WFS",
            "medwaymaps.medway.gov.uk/wfs/wfs.exe",
        ),
        (ENDPOINT, None),
    ],
)
def test_service_root(endpoint_url, root):
    assert service_root(endpoint_url) == root


@pytest.mark.parametrize(
    "href",
    [
        ARCGIS,
        f"{ARCGIS}/",
        f"{ARCGIS}/1",
        f"{ARCGIS}?f=pjson",
        ARCGIS.replace("https://", "http://"),
    ],
)
def test_an_arcgis_endpoint_matches_a_link_to_its_service(href):
    assert hrefs_match(href, f"{ARCGIS}/0/query?where=1%3D1&f=geojson", PAGE)


def test_a_wfs_endpoint_matches_a_link_to_its_service():
    endpoint = f"{WFS}?service=WFS&request=GetFeature&typeName=a4"
    assert hrefs_match(f"{WFS}?service=WFS&request=GetCapabilities", endpoint, PAGE)


def test_an_arcgis_endpoint_does_not_match_another_service():
    other = ARCGIS.replace("/A4D/", "/A4D_old/")
    assert not hrefs_match(other, f"{ARCGIS}/0/query?f=geojson", PAGE)


def test_hrefs_match_rejects_a_different_file():
    assert not hrefs_match("/downloads/file/3150/timetable.csv", ENDPOINT, PAGE)


# -- fetch_page ---------------------------------------------------------------


def test_fetch_page_collects_anchor_hrefs(mocker):
    _serve(mocker, {PAGE: '<a href="/a.csv">A</a><link href="/style.css"><a>none</a>'})

    assert fetch_page(PAGE) == (PAGE, ["/a.csv"], None)


def test_fetch_page_returns_the_url_after_redirects(mocker):
    _serve(mocker, {PAGE: _Response("", url="https://council.gov.uk/new-page")})

    assert fetch_page(PAGE)[0] == "https://council.gov.uk/new-page"


@pytest.mark.parametrize(
    "page, error",
    [
        (_Response(status_code=403), "http-403"),
        (requests.exceptions.Timeout(), "timeout"),
        (requests.exceptions.ConnectionError("pool at 0x7f..."), "connection-error"),
    ],
)
def test_fetch_page_errors_are_fixed_categories(mocker, page, error):
    """The error feeds the task reference, so must not carry varying text."""
    _serve(mocker, {PAGE: page})

    assert fetch_page(PAGE) == (PAGE, [], error)


# -- check_endpoint -----------------------------------------------------------


def test_found_on_any_documentation_page():
    pages = {
        "https://a": ("https://a", ["/other.csv"], None),
        "https://b": ("https://b", [ENDPOINT], None),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result == {"result": FOUND, "matched_href": ENDPOINT}


def test_not_found_offers_the_data_file_links_on_the_page():
    pages = {
        PAGE: (
            PAGE,
            ["/downloads/file/9999/timetable.csv", "/about", "/data.XLSX"],
            None,
        )
    }

    result = check_endpoint(ENDPOINT, [PAGE], pages)

    assert result == {
        "result": NOT_FOUND,
        "fetch_errors": [],
        "redirects": {},
        "candidate_links": [
            "https://council.gov.uk/data.XLSX",
            "https://council.gov.uk/downloads/file/9999/timetable.csv",
        ],
        # same file name at a new URL, as a CMS gives each upload
        "likely_replacement": "https://council.gov.uk/downloads/file/9999/timetable.csv",
    }


def test_no_likely_replacement_without_the_same_file_name():
    pages = {PAGE: (PAGE, ["/downloads/file/9999/local-plan.csv"], None)}

    assert check_endpoint(ENDPOINT, [PAGE], pages)["likely_replacement"] == ""


def test_not_found_records_a_page_that_moved():
    """Dover's Article 4 page now goes to its conservation areas page."""
    moved = "https://council.gov.uk/heritage/conservation-areas"
    pages = {PAGE: (moved, [], None)}

    result = check_endpoint(ENDPOINT, [PAGE], pages)

    assert result["result"] == NOT_FOUND
    assert result["redirects"] == {PAGE: moved}


@pytest.mark.parametrize(
    "final",
    [
        "http://council.gov.uk/planning/local-plan",
        "https://www.council.gov.uk/planning/local-plan/",
    ],
)
def test_a_change_of_scheme_www_or_slash_is_not_a_move(final):
    pages = {PAGE: (final, [], None)}

    assert check_endpoint(ENDPOINT, [PAGE], pages)["redirects"] == {}


def test_not_found_when_only_some_pages_could_be_read():
    pages = {
        "https://a": ("https://a", [], "http-404"),
        "https://b": ("https://b", ["/other"], None),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result["result"] == NOT_FOUND
    assert result["fetch_errors"] == ["http-404"]


@pytest.mark.parametrize("error", ["http-404", "http-410"])
def test_gone_when_every_page_no_longer_exists(error):
    pages = {PAGE: (PAGE, [], error)}

    assert check_endpoint(ENDPOINT, [PAGE], pages) == {
        "result": DOCUMENTATION_URL_GONE,
        "fetch_errors": [error],
    }


def test_unreachable_when_a_page_is_blocked_and_another_gone():
    """A blocked page might still link to the endpoint, so it isn't known to be gone."""
    pages = {
        "https://a": ("https://a", [], "http-404"),
        "https://b": ("https://b", [], "http-403"),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result == {"result": UNREACHABLE, "fetch_errors": ["http-403", "http-404"]}


def test_unreachable_when_no_page_could_be_read():
    pages = {PAGE: (PAGE, [], "timeout")}

    assert check_endpoint(ENDPOINT, [PAGE], pages) == {
        "result": UNREACHABLE,
        "fetch_errors": ["timeout"],
    }


# -- check_endpoints ----------------------------------------------------------


def test_each_documentation_page_is_fetched_once(mocker):
    get = _serve(mocker, {PAGE: f'<a href="{ENDPOINT}">timetable</a>'})
    endpoints = [
        {"endpoint": "e1", "endpoint_url": ENDPOINT, "documentation_urls": [PAGE]},
        {
            "endpoint": "e2",
            "endpoint_url": ENDPOINT + "?v=2",
            "documentation_urls": [PAGE],
        },
    ]

    results = check_endpoints(endpoints, max_workers=2)

    assert get.call_count == 1
    assert [r["endpoint"] for r in results] == ["e1", "e2"]
    assert results[0]["result"] == FOUND
    # path-only matching ignores the query string, as Manage's does
    assert results[1]["result"] == FOUND
