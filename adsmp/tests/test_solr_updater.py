#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import math
import os
import re
import sys
import unittest
from datetime import datetime, timedelta
from io import BytesIO

import adsputils
import mock
from adsputils import get_date
from mock import patch

from adsmp import app, models, solr_updater
from adsmp.models import Base, Records


class TestSolrUpdater(unittest.TestCase):
    """
    Tests the appliction's methods
    """

    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        self.app = app.ADSMasterPipelineCelery(
            "test",
            local_config={
                "SQLALCHEMY_URL": "sqlite:///",
                "METRICS_SQLALCHEMY_URL": "sqlite:///",
                "SQLALCHEMY_ECHO": False,
                "PROJ_HOME": proj_home,
                "TEST_DIR": os.path.join(proj_home, "adsmp/tests"),
            },
        )
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_solr_transformer(self):
        """Makes sure we can write recs into the storage."""

        self.app.update_storage(
            "bibcode",
            "metadata",
            {
                "abstract": "abstract text",
                "aff": ["-", "-", "-", "-"],
                "alternate_bibcode": ["2003adass..12..283B"],
                "author": [
                    "Blecksmith, E.",
                    "Paltani, S.",
                    "Rots, A.",
                    "Winkelman, S.",
                ],
                "author_count": 4,
                "author_facet": [
                    "Blecksmith, E",
                    "Paltani, S",
                    "Rots, A",
                    "Winkelman, S",
                ],
                "author_facet_hier": [
                    "0/Blecksmith, E",
                    "1/Blecksmith, E/Blecksmith, E.",
                    "0/Paltani, S",
                    "1/Paltani, S/Paltani, S.",
                    "0/Rots, A",
                    "1/Rots, A/Rots, A.",
                    "0/Winkelman, S",
                    "1/Winkelman, S/Winkelman, S.",
                ],
                "author_norm": [
                    "Blecksmith, E",
                    "Paltani, S",
                    "Rots, A",
                    "Winkelman, S",
                ],
                "bibcode": "2003ASPC..295..283B",
                "bibgroup": ["bibCXC", "CfA"],
                "bibgroup_facet": ["bibCXC", "CfA"],
                "bibstem": ["ASPC", "ASPC..295"],
                "bibstem_facet": "ASPC",
                "database": ["astronomy"],
                "date": "2003-01-01T00:00:00.000000Z",
                "doctype": "inproceedings",
                "doctype_facet_hier": ["0/Article", "1/Article/Proceedings Article"],
                "editor": ["Testeditor, Z."],
                "email": ["-", "-", "-", "-"],
                "first_author": "Blecksmith, E.",
                "first_author_facet_hier": [
                    "0/Blecksmith, E",
                    "1/Blecksmith, E/Blecksmith, E.",
                ],
                "first_author_norm": "Blecksmith, E",
                "id": "1401492",
                "identifier": ["2003adass..12..283B"],
                "links_data": "",  ### TODO(rca): superconfusing string, but fortunately we are getting ridd of it
                "orcid_pub": ["-", "-", "-", "-"],
                "page": ["283"],
                # u'property': [u'OPENACCESS', u'ADS_OPENACCESS', u'ARTICLE', u'NOT REFEREED'],
                "pub": "Astronomical Data Analysis Software and Systems XII",
                "pub_raw": "Astronomical Data Analysis Software and Systems XII ASP Conference Series, Vol. 295, 2003 H. E. Payne, R. I. Jedrzejewski, and R. N. Hook, eds., p.283",
                "pubdate": "2003-00-00",
                "title": ["Chandra Data Archive Download and Usage Database"],
                "volume": "295",
                "year": "2003",
            },
        )
        self.app.update_storage(
            "bibcode",
            "fulltext",
            {
                "body": "texttext",
                "acknowledgements": "aaa",
                "dataset": ["a", "b", "c"],
                "facility": ["fac1", "fac2", "fac3"],
            },
        )
        self.app.update_storage(
            "bibcode",
            "metrics",
            {
                "downloads": [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    2,
                    1,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    1,
                    2,
                ],
                "bibcode": "2003ASPC..295..361M",
                "reads": [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0,
                    4,
                    2,
                    5,
                    1,
                    0,
                    0,
                    1,
                    0,
                    0,
                    2,
                    4,
                    5,
                ],
                "author_num": 2,
            },
        )
        self.app.update_storage(
            "bibcode",
            "orcid_claims",
            {
                "authors": [
                    "Blecksmith, E.",
                    "Paltani, S.",
                    "Rots, A.",
                    "Winkelman, S.",
                ],
                "bibcode": "2003ASPC..295..283B",
                "unverified": ["-", "-", "0000-0003-2377-2356", "-"],
            },
        )
        self.app.update_storage(
            "bibcode",
            "metrics",
            {
                "citation_num": 6,
                "citations": [
                    "2007ApPhL..91g1118P",
                    "2010ApPhA..99..805K",
                    "2011TSF...520..610L",
                    "2012NatCo...3E1175B",
                    "2014IPTL...26..305A",
                    "2016ITED...63..197G",
                ],
            },
        )
        self.app.update_storage(
            "bibcode",
            "nonbib_data",
            {
                "authors": [
                    "Zaus, E",
                    "Tedde, S",
                    "Fuerst, J",
                    "Henseler, D",
                    "Doehler, G",
                ],
                "bibcode": "2007JAP...101d4501Z",
                "bibgroup": ["CXC", "CfA"],
                "bibgroup_facet": ["CXC", "CfA"],
                "boost": 0.1899999976158142,
                "data": ["MAST:3", "SIMBAD:1"],
                "property": ["OPENACCESS", "ADS_OPENACCESS", "ARTICLE", "NOT REFEREED"],
                "downloads": [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
                "id": 7862455,
                "norm_cites": 4225,
                "reads": [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    4,
                    6,
                    2,
                    1,
                    0,
                    0,
                    1,
                    0,
                    1,
                    0,
                    0,
                ],
                "refereed": True,
                "reference": [
                    "1977JAP....48.4729M",
                    "1981psd..book.....S",
                    "1981wi...book.....S",
                    "1986PhRvB..33.5545M",
                    "1987ApPhL..51..913T",
                    "1992Sci...258.1474S",
                    "1994IJMPB...8..237S",
                    "1995Natur.376..498H",
                    "1995Sci...270.1789Y",
                    "1998TSF...331...76O",
                    "1999Natur.397..121F",
                    "2000JaJAP..39...94P",
                    "2002ApPhL..81.3885S",
                    "2004ApPhL..85.3890C",
                    "2004TSF...451..105S",
                    "2005PhRvB..72s5208M",
                    "2006ApPhL..89l3505L",
                ],
                "simbad_objects": ["2419335 sim", "3111723 sim*"],
                "ned_objects": ["2419335 HII", "3111723 ned*"],
                "grants": ["2419335 g", "3111723 g*"],
                "citation_count": 6,
                "citation_count_norm": 0.2,
            },
        )
        rec = self.app.get_record("bibcode")

        x = solr_updater.transform_json_record(rec)
        # self.assertFalse('aff' in x, 'virtual field should not be in solr output')

        self.assertTrue(
            x["aff"] == rec["bib_data"]["aff"],
            "solr record should include aff from bib data when augment is not available",
        )
        self.assertFalse(
            "aff_abbrev" in x,
            "augment field should not be in solr record when augment is not available",
        )

        self.assertEqual(
            x["has"],
            [
                "abstract",
                "ack",
                "author",
                "bibgroup",
                "body",
                "citation_count",
                "database",
                "doctype",
                "first_author",
                "identifier",
                "orcid_other",
                "property",
                "pub",
                "pub_raw",
                "title",
                "volume",
            ],
        )
        self.assertEqual(x["scix_id"], "scix:0000-0000-0011")
        self.assertEqual(round(x["doctype_boost"],3),0.857)

        self.app.update_storage(
            "bibcode",
            "augment",
            {
                "aff": ["augment pipeline aff", "-", "-", "-"],
                "aff_abbrev": ["-", "-", "-", "-"],
                "aff_canonical": ["-", "-", "-", "-"],
                "aff_facet": ["-", "-", "-", "-"],
                "aff_facet_hier": ["-", "-", "-", "-"],
                "aff_id": ["-", "-", "-", "-"],
                "institution": ["-", "-", "-", "-"],
            },
        )

        rec = self.app.get_record("bibcode")
        self.assertDictContainsSubset(
            {
                "abstract": "abstract text",
                "ack": "aaa",
                "aff_abbrev": ["-", "-", "-", "-"],
                "aff_canonical": ["-", "-", "-", "-"],
                "aff_facet": ["-", "-", "-", "-"],
                "aff_facet_hier": ["-", "-", "-", "-"],
                "aff_id": ["-", "-", "-", "-"],
                "institution": ["-", "-", "-", "-"],
                "alternate_bibcode": ["2003adass..12..283B"],
                "author": [
                    "Blecksmith, E.",
                    "Paltani, S.",
                    "Rots, A.",
                    "Winkelman, S.",
                ],
                "author_count": 4,
                "author_facet": [
                    "Blecksmith, E",
                    "Paltani, S",
                    "Rots, A",
                    "Winkelman, S",
                ],
                "author_facet_hier": [
                    "0/Blecksmith, E",
                    "1/Blecksmith, E/Blecksmith, E.",
                    "0/Paltani, S",
                    "1/Paltani, S/Paltani, S.",
                    "0/Rots, A",
                    "1/Rots, A/Rots, A.",
                    "0/Winkelman, S",
                    "1/Winkelman, S/Winkelman, S.",
                ],
                "author_norm": [
                    "Blecksmith, E",
                    "Paltani, S",
                    "Rots, A",
                    "Winkelman, S",
                ],
                "bibcode": "2003ASPC..295..283B",
                "bibgroup": ["CXC", "CfA"],
                "bibgroup_facet": ["CXC", "CfA"],
                "bibstem": ["ASPC", "ASPC..295"],
                "bibstem_facet": "ASPC",
                "body": "texttext",
                "citation": [
                    "2007ApPhL..91g1118P",
                    "2010ApPhA..99..805K",
                    "2011TSF...520..610L",
                    "2012NatCo...3E1175B",
                    "2014IPTL...26..305A",
                    "2016ITED...63..197G",
                ],
                "citation_count": 6,
                "citation_count_norm": 0.2,
                "cite_read_boost": 0.1899999976158142,
                "data": ["MAST:3", "SIMBAD:1"],
                "data_facet": ["MAST", "SIMBAD"],
                "database": ["astronomy"],
                # u'dataset': ['a', 'b', 'c'],
                "date": "2003-01-01T00:00:00.000000Z",
                "doctype": "inproceedings",
                "doctype_facet_hier": ["0/Article", "1/Article/Proceedings Article"],
                "editor": ["Testeditor, Z."],
                "email": ["-", "-", "-", "-"],
                "facility": ["fac1", "fac2", "fac3"],
                "first_author": "Blecksmith, E.",
                "first_author_facet_hier": [
                    "0/Blecksmith, E",
                    "1/Blecksmith, E/Blecksmith, E.",
                ],
                "first_author_norm": "Blecksmith, E",
                "id": 1,  # from id in master database records table
                "identifier": ["2003adass..12..283B"],
                "links_data": "",
                "orcid_other": ["-", "-", "0000-0003-2377-2356", "-"],
                "orcid_pub": ["-", "-", "-", "-"],
                "nedid": ["2419335", "3111723"],
                "nedtype": ["HII Region", "Other"],
                "ned_object_facet_hier": [
                    "0/HII Region",
                    "1/HII Region/2419335",
                    "0/Other",
                    "1/Other/3111723",
                ],
                "page": ["283"],
                "property": ["OPENACCESS", "ADS_OPENACCESS", "ARTICLE", "NOT REFEREED"],
                "pub": "Astronomical Data Analysis Software and Systems XII",
                "pub_raw": "Astronomical Data Analysis Software and Systems XII ASP Conference Series, Vol. 295, 2003 H. E. Payne, R. I. Jedrzejewski, and R. N. Hook, eds., p.283",
                "pubdate": "2003-00-00",
                "read_count": 0,
                "reference": [
                    "1977JAP....48.4729M",
                    "1981psd..book.....S",
                    "1981wi...book.....S",
                    "1986PhRvB..33.5545M",
                    "1987ApPhL..51..913T",
                    "1992Sci...258.1474S",
                    "1994IJMPB...8..237S",
                    "1995Natur.376..498H",
                    "1995Sci...270.1789Y",
                    "1998TSF...331...76O",
                    "1999Natur.397..121F",
                    "2000JaJAP..39...94P",
                    "2002ApPhL..81.3885S",
                    "2004ApPhL..85.3890C",
                    "2004TSF...451..105S",
                    "2005PhRvB..72s5208M",
                    "2006ApPhL..89l3505L",
                ],
                "simbid": ["2419335", "3111723"],
                "simbtype": ["Other", "Star"],
                "simbad_object_facet_hier": [
                    "0/Other",
                    "1/Other/2419335",
                    "0/Star",
                    "1/Star/3111723",
                ],
                "title": ["Chandra Data Archive Download and Usage Database"],
                "volume": "295",
                "year": "2003",
            },
            solr_updater.transform_json_record(rec),
        )

        for x in Records._date_fields:
            if x in rec:
                rec[x] = get_date("2017-09-19T21:17:12.026474+00:00")

        x = solr_updater.transform_json_record(rec)
        for f in (
            "metadata_mtime",
            "fulltext_mtime",
            "orcid_mtime",
            "nonbib_mtime",
            "metrics_mtime",
            "update_timestamp",
        ):
            self.assertEqual(x[f], "2017-09-19T21:17:12.026474Z")

        rec["orcid_claims_updated"] = get_date("2017-09-20T21:17:12.026474+00:00")
        x = solr_updater.transform_json_record(rec)
        for f in (
            "metadata_mtime",
            "fulltext_mtime",
            "orcid_mtime",
            "nonbib_mtime",
            "metrics_mtime",
            "update_timestamp",
        ):
            if f == "update_timestamp" or f == "orcid_mtime":
                self.assertEqual(x[f], "2017-09-20T21:17:12.026474Z")
            else:
                self.assertEqual(x[f], "2017-09-19T21:17:12.026474Z")

        rec = self.app.get_record("bibcode")
        x = solr_updater.transform_json_record(rec)

        self.assertTrue("aff" in x)  # aff is no longer a virtual field
        self.assertEqual(
            x["aff"], rec["augments"]["aff"]
        )  # solr record should prioritize aff data from augment
        self.assertEqual(
            x["aff_abbrev"], rec["augments"]["aff_abbrev"]
        )  # solr record should include augment data
        self.assertEqual(x["bibgroup"], rec["nonbib_data"]["bibgroup"])
        self.assertEqual(x["bibgroup_facet"], rec["nonbib_data"]["bibgroup_facet"])
        self.assertEqual(
            x["has"],
            [
                "abstract",
                "ack",
                "aff",
                "author",
                "bibgroup",
                "body",
                "citation_count",
                "database",
                "doctype",
                "first_author",
                "identifier",
                "orcid_other",
                "property",
                "pub",
                "pub_raw",
                "title",
                "volume",
            ],
        )
        self.assertEqual(round(x["doctype_boost"],3),0.857)


    def test_links_data_merge(self):
        # links_data only from bib
        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": ['{"url": "http://asdf"}']},
            "bib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertEqual(db_record["bib_data"]["links_data"], solr_record["links_data"])
        self.assertEqual(solr_record["scix_id"], None)
        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": ['{"url": "http://asdf"}']},
            "bib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertEqual(db_record["bib_data"]["links_data"], solr_record["links_data"])

        # links_data only from nonbib
        db_record = {
            "bibcode": "foo",
            "nonbib_data": {"links_data": "asdf"},
            "nonbib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertEqual(
            db_record["nonbib_data"]["links_data"], solr_record["links_data"]
        )

        # links_data from both
        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": "asdf"},
            "bib_data_updated": datetime.now(),
            "nonbib_data": {"links_data": "jkl"},
            "nonbib_data_updated": datetime.now() - timedelta(1),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertEqual(
            db_record["nonbib_data"]["links_data"], solr_record["links_data"]
        )
        self.assertEqual(solr_record["has"], [])

        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": "asdf"},
            "bib_data_updated": datetime.now() - timedelta(1),
            "nonbib_data": {"links_data": "jkl"},
            "nonbib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertEqual(
            db_record["nonbib_data"]["links_data"], solr_record["links_data"]
        )

        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": ['{"url": "http://foo", "access": "open"}']},
            "bib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertTrue("ESOURCE" in solr_record["property"])
        # verify all values are populated
        self.assertTrue("ARTICLE" in solr_record["property"])
        self.assertTrue("NOT REFEREED" in solr_record["property"])
        self.assertTrue("EPRINT_OPENACCESS" in solr_record["property"])
        self.assertTrue("OPENACCESS" in solr_record["property"])
        self.assertTrue("EPRINT_HTML" in solr_record["esources"])
        self.assertTrue("EPRINT_PDF" in solr_record["esources"])

        db_record = {
            "bibcode": "foo",
            "bib_data": {"links_data": ['{"url": "http://foo", "access": "closed"}']},
            "bib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertTrue("ESOURCE" not in solr_record["property"])

        db_record = {
            "bibcode": "foo",
            "bib_data": {},
            "bib_data_updated": datetime.now(),
        }
        solr_record = solr_updater.transform_json_record(db_record)
        self.assertTrue("property" not in solr_record)

    def test_extract_data_pipeline(self):
        nonbib = {
            "simbad_objects": ["947046 "],
            "ned_objects": ["MESSIER_031 G", "SN_1885A "],
        }
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(["947046"], d["simbid"])
        self.assertEqual(["Other"], d["simbtype"])
        self.assertEqual(["0/Other", "1/Other/947046"], d["simbad_object_facet_hier"])
        self.assertEqual(["MESSIER_031", "SN_1885A"], d["nedid"])
        self.assertEqual(["Galaxy", "Other"], d["nedtype"])
        self.assertEqual(
            ["0/Galaxy", "1/Galaxy/MESSIER_031", "0/Other", "1/Other/SN_1885A"],
            d["ned_object_facet_hier"],
        )

        nonbib = {
            "simbad_objects": ["947046"],
            "ned_objects": ["MESSIER_031 G", "SN_1885A"],
        }
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(["947046"], d["simbid"])
        self.assertEqual(["Other"], d["simbtype"])
        self.assertEqual(["0/Other", "1/Other/947046"], d["simbad_object_facet_hier"])
        self.assertEqual(["MESSIER_031", "SN_1885A"], d["nedid"])
        self.assertEqual(["Galaxy", "Other"], d["nedtype"])
        self.assertEqual(
            ["0/Galaxy", "1/Galaxy/MESSIER_031", "0/Other", "1/Other/SN_1885A"],
            d["ned_object_facet_hier"],
        )

        # Test simple planetary_feature
        nonbib = {"planetary_feature": ["Moon/Crater/Langrenus/3273"]}
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(["Moon/Crater/Langrenus"], d["planetary_feature"])
        self.assertEqual(["3273"], d["planetary_feature_id"])
        self.assertEqual(
            ["0/Moon", "1/Moon/Crater", "2/Moon/Crater/Langrenus"],
            d["planetary_feature_facet_hier_3level"],
        )
        self.assertEqual(
            ["0/Moon", "1/Moon/Crater Langrenus"],
            d["planetary_feature_facet_hier_2level"],
        )

        # Test planetary_feature with space in feature name
        nonbib = {"planetary_feature": ["Mars/Terra/Terra Cimmeria/5930"]}
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(["Mars/Terra/Terra Cimmeria"], d["planetary_feature"])
        self.assertEqual(["5930"], d["planetary_feature_id"])
        self.assertEqual(
            ["0/Mars", "1/Mars/Terra", "2/Mars/Terra/Terra Cimmeria"],
            d["planetary_feature_facet_hier_3level"],
        )
        self.assertEqual(
            ["0/Mars", "1/Mars/Terra Cimmeria"],
            d["planetary_feature_facet_hier_2level"],
        )

        # Test one bibcode with multiple planetary_features assigned
        nonbib = {
            "planetary_feature": [
                "Moon/Mare/Mare Imbrium/3678",
                "Moon/Crater/Alder/171",
                "Moon/Crater/Finsen/1959",
                "Moon/Crater/Leibnitz/3335",
            ]
        }
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(
            [
                "Moon/Mare/Mare Imbrium",
                "Moon/Crater/Alder",
                "Moon/Crater/Finsen",
                "Moon/Crater/Leibnitz",
            ],
            d["planetary_feature"],
        )
        self.assertEqual(["3678", "171", "1959", "3335"], d["planetary_feature_id"])
        self.assertEqual(
            [
                "0/Moon",
                "1/Moon/Mare",
                "2/Moon/Mare/Mare Imbrium",
                "0/Moon",
                "1/Moon/Crater",
                "2/Moon/Crater/Alder",
                "0/Moon",
                "1/Moon/Crater",
                "2/Moon/Crater/Finsen",
                "0/Moon",
                "1/Moon/Crater",
                "2/Moon/Crater/Leibnitz",
            ],
            d["planetary_feature_facet_hier_3level"],
        )
        self.assertEqual(
            [
                "0/Moon",
                "1/Moon/Mare Imbrium",
                "0/Moon",
                "1/Moon/Crater Alder",
                "0/Moon",
                "1/Moon/Crater Finsen",
                "0/Moon",
                "1/Moon/Crater Leibnitz",
            ],
            d["planetary_feature_facet_hier_2level"],
        )

        # Test uat
        nonbib = {
            "uat": [
                "cosmology/origin of the universe/early universe/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation/322",
                "cosmology/origin of the universe/big bang theory/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation/322",
                "observational astronomy/astronomical methods/radio astronomy/cosmic noise/cosmic background radiation/cosmic microwave background radiation/322",
                "cosmology/astronomical radiation sources/radio sources/radio continuum emission/5",
                "interstellar medium/interstellar emissions/radio continuum emission/5",
                "stellar astronomy/stellar types/stellar evolutionary types/evolved stars/subgiant stars/1646",
            ]
        }
        d = solr_updater.extract_data_pipeline(nonbib, None)
        self.assertEqual(
            [
                "cosmology/origin of the universe/early universe/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation",
                "cosmology/origin of the universe/big bang theory/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation",
                "observational astronomy/astronomical methods/radio astronomy/cosmic noise/cosmic background radiation/cosmic microwave background radiation",
                "cosmology/astronomical radiation sources/radio sources/radio continuum emission",
                "interstellar medium/interstellar emissions/radio continuum emission",
                "stellar astronomy/stellar types/stellar evolutionary types/evolved stars/subgiant stars",
            ],
            d["uat"],
        )
        self.assertEqual(["322", "322", "322", "5", "5", "1646"], d["uat_id"])
        self.assertEqual(
            [
                "0/cosmology",
                "1/cosmology/origin of the universe",
                "2/cosmology/origin of the universe/early universe",
                "3/cosmology/origin of the universe/early universe/recombination (cosmology)",
                "4/cosmology/origin of the universe/early universe/recombination (cosmology)/cosmic background radiation",
                "5/cosmology/origin of the universe/early universe/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation",
                "0/cosmology",
                "1/cosmology/origin of the universe",
                "2/cosmology/origin of the universe/big bang theory",
                "3/cosmology/origin of the universe/big bang theory/recombination (cosmology)",
                "4/cosmology/origin of the universe/big bang theory/recombination (cosmology)/cosmic background radiation",
                "5/cosmology/origin of the universe/big bang theory/recombination (cosmology)/cosmic background radiation/cosmic microwave background radiation",
                "0/observational astronomy",
                "1/observational astronomy/astronomical methods",
                "2/observational astronomy/astronomical methods/radio astronomy",
                "3/observational astronomy/astronomical methods/radio astronomy/cosmic noise",
                "4/observational astronomy/astronomical methods/radio astronomy/cosmic noise/cosmic background radiation",
                "5/observational astronomy/astronomical methods/radio astronomy/cosmic noise/cosmic background radiation/cosmic microwave background radiation",
                "0/cosmology",
                "1/cosmology/astronomical radiation sources",
                "2/cosmology/astronomical radiation sources/radio sources",
                "3/cosmology/astronomical radiation sources/radio sources/radio continuum emission",
                "0/interstellar medium",
                "1/interstellar medium/interstellar emissions",
                "2/interstellar medium/interstellar emissions/radio continuum emission",
                "0/stellar astronomy",
                "1/stellar astronomy/stellar types",
                "2/stellar astronomy/stellar types/stellar evolutionary types",
                "3/stellar astronomy/stellar types/stellar evolutionary types/evolved stars",
                "4/stellar astronomy/stellar types/stellar evolutionary types/evolved stars/subgiant stars",
            ],
            d["uat_facet_hier"],
        )


if __name__ == "__main__":
    unittest.main()
