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
from jobs.config.licence_task import OGL_FOUND, OGL_NOT_FOUND
from jobs.utils import documentation_url
from jobs.utils.documentation_url import (
    FOUND,
    _date_in_name,
    check_endpoint,
    check_endpoints,
    check_licence,
    fetch_page,
    hrefs_match,
    likely_replacement_for_dataset,
    mentions_ogl,
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

    assert fetch_page(PAGE) == (PAGE, ["/a.csv"], None, False)


def test_fetch_page_unwraps_an_outlook_safe_link(mocker):
    """North Norfolk's timetable link was copied from an email."""
    href = (
        "https://eur03.safelinks.protection.outlook.com/?url=https%3A%2F%2F"
        "www.north-norfolk.gov.uk%2Fmedia%2Flocalplan%2Ftimetable.csv"
        "&amp;data=05%7C02%7C&amp;reserved=0"
    )
    _serve(mocker, {PAGE: f'<a href="{href}">timetable</a>'})

    assert fetch_page(PAGE)[1] == [
        "https://www.north-norfolk.gov.uk/media/localplan/timetable.csv"
    ]


@pytest.mark.parametrize(
    "href",
    [
        # a url parameter on any other site is left alone
        "https://council.gov.uk/redirect?url=https%3A%2F%2Felsewhere.com%2Fa.csv",
        # a Safe Link with no URL in it
        "https://eur03.safelinks.protection.outlook.com/?data=05",
    ],
)
def test_fetch_page_leaves_other_links_as_they_are(mocker, href):
    _serve(mocker, {PAGE: f'<a href="{href}">link</a>'})

    assert fetch_page(PAGE)[1] == [href]


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

    assert fetch_page(PAGE) == (PAGE, [], error, False)


def test_fetch_page_finds_an_ogl_statement_in_the_footer(mocker):
    _serve(
        mocker,
        {
            PAGE: "<main><a href='/a.csv'>Data</a></main>"
            "<footer>All content is available under the "
            "<a href='https://www.nationalarchives.gov.uk/doc/open-government-licence/"
            "version/3/'>Open Government Licence v3.0</a></footer>"
        },
    )

    assert fetch_page(PAGE)[3] is True


def test_fetch_page_without_an_ogl_statement(mocker):
    _serve(mocker, {PAGE: "<p>Download our conservation areas.</p>"})

    assert fetch_page(PAGE)[3] is False


# -- mentions_ogl -------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "The data is provided under the Open Government Licence v3",
        "Licensed under the open government licence",
        "Available under the Open\n   Government\xa0Licence version 3.0",
        "Released under the Open Government License",
        "Published under OGL v3.0",
        "© Crown copyright. OGL",
    ],
)
def test_mentions_ogl_by_name_or_abbreviation(text):
    assert mentions_ogl(text, [])


@pytest.mark.parametrize(
    "href",
    [
        "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "http://www.nationalarchives.gov.uk/doc/open-government-licence/",
        "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3",
    ],
)
def test_mentions_ogl_by_link(href):
    assert mentions_ogl("", [href])


@pytest.mark.parametrize(
    "text",
    [
        "Download our conservation areas as a CSV.",
        "Contact the government team about your licence application.",
        "Google maps",
        "boglands",
    ],
)
def test_does_not_mention_ogl(text):
    assert not mentions_ogl(text, ["/about", "https://council.gov.uk/licensing"])


def test_scripts_and_styles_are_not_page_text(mocker):
    _serve(
        mocker,
        {PAGE: "<script>var l = 'Open Government Licence';</script><p>Data</p>"},
    )

    assert fetch_page(PAGE)[3] is False


def test_an_ogl_logo_alt_text_counts(mocker):
    _serve(mocker, {PAGE: '<img src="/ogl.svg" alt="Open Government Licence">'})

    assert fetch_page(PAGE)[3] is True


# -- check_licence ------------------------------------------------------------


def test_licence_found_on_any_documentation_page():
    pages = {
        "https://a": ("https://a", [], None, False),
        "https://b": ("https://b", [], None, True),
    }

    assert check_licence(["https://a", "https://b"], pages) == {"result": OGL_FOUND}


def test_licence_not_found_when_a_page_was_read():
    pages = {
        "https://a": ("https://a", [], "http-403", False),
        "https://b": ("https://b", [], None, False),
    }

    assert check_licence(["https://a", "https://b"], pages) == {
        "result": OGL_NOT_FOUND,
        "fetch_errors": ["http-403"],
    }


def test_licence_unknown_when_no_page_could_be_read():
    """The documentation URL task already reports an unreadable page."""
    pages = {PAGE: (PAGE, [], "timeout", False)}

    assert check_licence([PAGE], pages) is None


# -- check_endpoint -----------------------------------------------------------


def test_found_on_any_documentation_page():
    pages = {
        "https://a": ("https://a", ["/other.csv"], None, False),
        "https://b": ("https://b", [ENDPOINT], None, False),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result == {"result": FOUND, "matched_href": ENDPOINT}


def test_not_found_offers_the_data_file_links_on_the_page():
    pages = {
        PAGE: (
            PAGE,
            ["/downloads/file/9999/timetable.csv", "/about", "/data.XLSX"],
            None,
            False,
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
    pages = {PAGE: (PAGE, ["/downloads/file/9999/local-plan.csv"], None, False)}

    assert check_endpoint(ENDPOINT, [PAGE], pages)["likely_replacement"] == ""


@pytest.mark.parametrize(
    "endpoint, link",
    [
        # Cheshire West and Chester: we collect the _0 copy, the page links the new one
        (
            "files/2026-10/local-plan-timetable_0.csv",
            "/files/2026-11/local-plan-timetable.csv",
        ),
        (
            "files/2026-10/local-plan-timetable.csv",
            "/files/2026-11/local-plan-timetable_0.csv",
        ),
        (
            "files/2026-10/local-plan-timetable.csv",
            "/files/2026-11/local-plan-timetable_12.csv",
        ),
    ],
)
def test_likely_replacement_allows_for_drupal_renaming_an_upload(endpoint, link):
    pages = {PAGE: (PAGE, [link], None, False)}

    result = check_endpoint(f"https://council.gov.uk/{endpoint}", [PAGE], pages)

    assert result["likely_replacement"] == f"https://council.gov.uk{link}"


def test_likely_replacement_prefers_the_exact_file_name():
    pages = {PAGE: (PAGE, ["/a/timetable_0.csv", "/b/timetable.csv"], None, False)}

    assert (
        check_endpoint("https://council.gov.uk/old/timetable.csv", [PAGE], pages)[
            "likely_replacement"
        ]
        == "https://council.gov.uk/b/timetable.csv"
    )


@pytest.mark.parametrize(
    "endpoint, link",
    [
        # the two parts of a brownfield land register are different files
        ("register_part_1.csv", "/register_part_2.csv"),
        ("timetable_0.csv", "/timetable_1.csv"),
        ("register_2022.xlsx", "/register_2025.xlsx"),
        ("register_2025.csv", "/register.csv"),
    ],
)
def test_two_numbered_names_are_not_a_likely_replacement(endpoint, link):
    pages = {PAGE: (PAGE, [link], None, False)}

    result = check_endpoint(f"https://council.gov.uk/old/{endpoint}", [PAGE], pages)

    assert result["likely_replacement"] == ""


# -- likely_replacement_for_dataset -------------------------------------------

DATASET_NAMES = {
    "local-plan": ["local-plan"],
    "plan-timetable": ["plan-timetable", "local-plan-timetable"],
    "minerals-plan": ["minerals-plan"],
    "brownfield-land": [
        "brownfield-land",
        "brownfield-register",
        "blr",
        "brownfieldregister",
    ],
}
FILES = "https://council.gov.uk/sites/default/files"
CHESHIRE_WEST = [
    f"{FILES}/2026-11/cheshire-west-and-chester-local-plan-timetable.csv",
    f"{FILES}/2026-11/cheshire-west-and-chester-local-plan.csv",
]
NORTH_DEVON = [
    f"{FILES}/2026-06/Local-Plan-CSV-North-Devon.csv",
    f"{FILES}/2026-06/Local-Plan-CSV-Torridge.csv",
    f"{FILES}/2026-11/Local-Plan-Timetable-3-Nov-26.csv",
]


@pytest.mark.parametrize(
    "dataset, candidate_links, expected",
    [
        # a local plan timetable file is plan-timetable's, not local-plan's
        ("local-plan", CHESHIRE_WEST, CHESHIRE_WEST[1]),
        ("plan-timetable", CHESHIRE_WEST, CHESHIRE_WEST[0]),
        ("plan-timetable", NORTH_DEVON, NORTH_DEVON[2]),
        # Chesterfield: a version run on to the word, timetablev3
        (
            "plan-timetable",
            [f"{FILES}/local-plan-timetablev3.csv", f"{FILES}/local-plan.csv"],
            f"{FILES}/local-plan-timetablev3.csv",
        ),
        ("local-plan", [f"{FILES}/local-plan-timetablev3.csv"], ""),
        # two local plan files: choosing between them needs a person
        ("local-plan", NORTH_DEVON, ""),
        # the page has nothing for this dataset
        ("minerals-plan", CHESHIRE_WEST, ""),
        # a task whose dataset could not be attributed
        ("", CHESHIRE_WEST, ""),
    ],
)
def test_likely_replacement_for_dataset(dataset, candidate_links, expected):
    assert (
        likely_replacement_for_dataset(
            dataset, f"{FILES}/2026-10/data.csv", candidate_links, DATASET_NAMES
        )
        == expected
    )


@pytest.mark.parametrize(
    "link",
    [
        "/BrownfieldLandRegister2024.csv",
        "/Brownfield_register_2025.csv",
        "/BLR2026.csv",
        "/brownfield%20land%20registers%202025.csv",
    ],
)
def test_likely_replacement_for_dataset_finds_the_name_however_written(link):
    candidate_links = [f"https://council.gov.uk{link}"]

    assert (
        likely_replacement_for_dataset(
            "brownfield-land", ENDPOINT, candidate_links, DATASET_NAMES
        )
        == candidate_links[0]
    )


def test_likely_replacement_for_dataset_prefers_the_endpoints_file_type():
    candidate_links = [
        f"{FILES}/brownfield-land-register-2025.xlsx",
        f"{FILES}/brownfield-land-register-2025.csv",
    ]

    assert (
        likely_replacement_for_dataset(
            "brownfield-land",
            f"{FILES}/brownfield-land-register-2021.csv",
            candidate_links,
            DATASET_NAMES,
        )
        == candidate_links[1]
    )


@pytest.mark.parametrize(
    "endpoint, candidate_links, expected",
    [
        # Cherwell: each year's register, in CSV and XLSX
        (
            f"{FILES}/brownfield-land-register-december-2017.csv",
            [
                f"{FILES}/brownfield-land-register-january-2023-csv.csv",
                f"{FILES}/brownfield-land-register-january-2023-xls.xlsx",
                f"{FILES}/brownfield-land-register-january-2024-updated-final.xlsx",
                f"{FILES}/brownfield-land-register-january-2024-updated-final.csv",
                f"{FILES}/brownfield-land-register-2025-csv.csv",
            ],
            f"{FILES}/brownfield-land-register-2025-csv.csv",
        ),
        # Wirral: the latest is only published as XLSX
        (
            f"{FILES}/Wirral Brownfield Part1 Sites Dec2018.csv",
            [
                f"{FILES}/Wirral Brownfield Land Register 2023 csv_0.xlsx",
                f"{FILES}/Wirral Brownfield Land Register 2024 csv_0.xlsx",
                f"{FILES}/Wirral Brownfield Land Register 2025 csv.xlsx",
            ],
            f"{FILES}/Wirral Brownfield Land Register 2025 csv.xlsx",
        ),
        # Lake District: the same day, the endpoint's file type
        (
            f"{FILES}/ldnpa_brownfieldregister_2025-12-12-rev8.csv",
            [
                f"{FILES}/ldnpa_brownfieldregister_2025-12-15-rev8.csv",
                f"{FILES}/ldnpa_brownfieldregister_2025-12-15-rev8_friendly.xlsx",
            ],
            f"{FILES}/ldnpa_brownfieldregister_2025-12-15-rev8.csv",
        ),
        # East Hampshire: an endpoint with no file type, one file in two formats
        (
            "https://council.gov.uk/media/6377/download",
            [
                f"{FILES}/Brownfield land register 2025.xlsx",
                f"{FILES}/Brownfield land register 2025.csv",
            ],
            f"{FILES}/Brownfield land register 2025.csv",
        ),
        # Uttlesford: a day-first date, and an ArcGIS endpoint
        (
            "https://services.arcgis.com/x/arcgis/rest/services/BLR/FeatureServer/0",
            [
                f"{FILES}/Uttlesford_Brownfield_Land_Register-28.05.26.xlsx",
                f"{FILES}/Uttlesford_Brownfield_Land_Register-28.05.26.csv",
            ],
            f"{FILES}/Uttlesford_Brownfield_Land_Register-28.05.26.csv",
        ),
        # two different files of the same date: choosing needs a person
        (
            "https://council.gov.uk/media/6377/download",
            [
                f"{FILES}/brownfield-land-register-2025-part-1.csv",
                f"{FILES}/brownfield-land-register-2025-part-2.xlsx",
            ],
            "",
        ),
        # one file has no date, so which is latest is not known
        (
            f"{FILES}/brownfield-land-register-2021.csv",
            [
                f"{FILES}/brownfield-land-register-2025.csv",
                f"{FILES}/brownfield-land-register-current.csv",
            ],
            "",
        ),
    ],
)
def test_likely_replacement_for_dataset_chooses_the_latest_then_the_csv(
    endpoint, candidate_links, expected
):
    assert (
        likely_replacement_for_dataset(
            "brownfield-land", endpoint, candidate_links, DATASET_NAMES
        )
        == expected
    )


@pytest.mark.parametrize(
    "name, expected",
    [
        ("register-2025.csv", (2025, 0, 0)),
        ("register-202211.xls", (2022, 11, 0)),
        ("register_2025-12-15-rev8.csv", (2025, 12, 15)),
        ("register-20200921.csv", (2020, 9, 21)),
        ("register-28.05.26.csv", (2026, 5, 28)),
        ("register-january-2024-final.csv", (2024, 1, 0)),
        ("Sites Dec2018.csv", (2018, 12, 0)),
        ("BrownfieldRegister2025-26.csv", (2025, 0, 0)),
        # the later of two dates
        ("register-2021-updated-2023.csv", (2023, 0, 0)),
        ("register-current.csv", None),
        ("c2f881_85f15d830dad4ed48229c181ec3917e3.xlsx", None),
    ],
)
def test_date_in_name(name, expected):
    assert _date_in_name(f"{FILES}/{name}") == expected


def test_not_found_records_a_page_that_moved():
    """Dover's Article 4 page now goes to its conservation areas page."""
    moved = "https://council.gov.uk/heritage/conservation-areas"
    pages = {PAGE: (moved, [], None, False)}

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
    pages = {PAGE: (final, [], None, False)}

    assert check_endpoint(ENDPOINT, [PAGE], pages)["redirects"] == {}


def test_not_found_when_only_some_pages_could_be_read():
    pages = {
        "https://a": ("https://a", [], "http-404", False),
        "https://b": ("https://b", ["/other"], None, False),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result["result"] == NOT_FOUND
    assert result["fetch_errors"] == ["http-404"]


@pytest.mark.parametrize("error", ["http-404", "http-410"])
def test_gone_when_every_page_no_longer_exists(error):
    pages = {PAGE: (PAGE, [], error, False)}

    assert check_endpoint(ENDPOINT, [PAGE], pages) == {
        "result": DOCUMENTATION_URL_GONE,
        "fetch_errors": [error],
    }


def test_unreachable_when_a_page_is_blocked_and_another_gone():
    """A blocked page might still link to the endpoint, so it isn't known to be gone."""
    pages = {
        "https://a": ("https://a", [], "http-404", False),
        "https://b": ("https://b", [], "http-403", False),
    }

    result = check_endpoint(ENDPOINT, ["https://a", "https://b"], pages)

    assert result == {"result": UNREACHABLE, "fetch_errors": ["http-403", "http-404"]}


def test_unreachable_when_no_page_could_be_read():
    pages = {PAGE: (PAGE, [], "timeout", False)}

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


def test_check_endpoints_checks_the_licence_from_the_same_fetch(mocker):
    get = _serve(
        mocker,
        {PAGE: f'<a href="{ENDPOINT}">timetable</a><footer>OGL v3.0</footer>'},
    )
    endpoints = [
        {"endpoint": "e1", "endpoint_url": ENDPOINT, "documentation_urls": [PAGE]}
    ]

    [result] = check_endpoints(endpoints, max_workers=1)

    assert get.call_count == 1
    assert result["result"] == FOUND
    assert result["licence"] == {"result": OGL_FOUND}
