# -*- coding: utf-8 -*-


from past.builtins import basestring
from adsputils import get_date
from datetime import datetime
from dateutil.tz import tzutc
from sqlalchemy import Column, Integer, BigInteger, String, Text, TIMESTAMP, Boolean, DateTime, ARRAY
from sqlalchemy import types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Enum
from sqlalchemy.dialects import postgresql
from sqlalchemy import text
import json

Base = declarative_base()
MetricsBase = declarative_base()


class UTCDateTime(types.TypeDecorator):
    impl = TIMESTAMP

    def process_bind_param(self, value, engine):
        if isinstance(value, basestring):
            return get_date(value).astimezone(tzutc())
        elif value is not None:
            return value.astimezone(tzutc())  # will raise Error is not datetime

    def process_result_value(self, value, engine):
        if value is not None:
            return datetime(value.year, value.month, value.day,
                            value.hour, value.minute, value.second,
                            value.microsecond, tzinfo=tzutc())


class KeyValue(Base):
    """Example model, it stores key/value pairs - a persistent configuration"""
    __tablename__ = 'storage'
    key = Column(String(255), primary_key=True)
    value = Column(Text)

    def toJSON(self):
        return {'key': self.key, 'value': self.value}


class Records(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19), index=True, unique=True)

    bib_data = Column(Text)  # 'metadata' is reserved by SQLAlchemy
    orcid_claims = Column(Text)
    nonbib_data = Column(Text)
    fulltext = Column(Text)
    metrics = Column(Text)
    # holds a dict of augments to be merged
    # currently only supported key is 'affiliations'
    #  with the value an array holding affiliation strings and '-' placeholders
    augments = Column(Text)
    collections = Column(ARRAY(String))

    # when data is received we set the updated timestamp
    bib_data_updated = Column(UTCDateTime, default=None)
    orcid_claims_updated = Column(UTCDateTime, default=None)
    nonbib_data_updated = Column(UTCDateTime, default=None)
    fulltext_updated = Column(UTCDateTime, default=None)
    metrics_updated = Column(UTCDateTime, default=None)
    augments_updated = Column(UTCDateTime, default=None)
    collections_updated = Column(UTCDateTime, default=None)

    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, default=get_date)
    processed = Column(UTCDateTime)

    solr_processed = Column(UTCDateTime, default=None)
    metrics_processed = Column(UTCDateTime, default=None)
    datalinks_processed = Column(UTCDateTime, default=None)

    solr_checksum = Column(String(10), default=None)
    metrics_checksum = Column(String(10), default=None)
    datalinks_checksum = Column(String(10), default=None)

    status = Column(Enum('solr-failed', 'metrics-failed', 'links-failed', 'retrying', 'success', name='status'))

    _date_fields = ['created', 'updated', 'processed',  # dates
                    'bib_data_updated', 'orcid_claims_updated', 'nonbib_data_updated',
                    'fulltext_updated', 'metrics_updated', 'augments_updated',
                    'datalinks_processed', 'solr_processed', 'metrics_processed']
    _text_fields = ['id', 'bibcode', 'status', 'solr_checksum', 'metrics_checksum', 'datalinks_checksum']
    _json_fields = ['bib_data', 'orcid_claims', 'nonbib_data', 'metrics', 'fulltext', 'augments']

    def toJSON(self, for_solr=False, load_only=None):
        if for_solr:
            return self
        else:
            load_only = load_only and set(load_only) or set()
            doc = {}

            for f in Records._text_fields:
                if load_only and f not in load_only:
                    continue
                doc[f] = getattr(self, f, None)
            for f in Records._date_fields:
                if load_only and f not in load_only:
                    continue
                if hasattr(self, f) and getattr(self, f):
                    doc[f] = get_date(getattr(self, f))
                else:
                    doc[f] = None
            for f in Records._json_fields:  # json
                if load_only and f not in load_only:
                    continue
                v = getattr(self, f, None)
                if v:
                    v = json.loads(v)
                doc[f] = v

            return doc


class ChangeLog(Base):
    __tablename__ = 'change_log'
    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    created = Column(UTCDateTime, default=get_date)
    key = Column(String(255))
    type = Column(String(255))
    oldvalue = Column(Text)
    permanent = Column(Boolean, default=False)

    def toJSON(self):
        return {'id': self.id,
                'key': self.key,
                'created': self.created and get_date(self.created).isoformat() or None,
                'oldvalue': self.oldvalue
                }


class IdentifierMapping(Base):
    """Storage for the mapping (bibcode translation) - it is a directed
    graph pointing to the most recent canonical identifier"""
    __tablename__ = 'identifiers'
    key = Column(String(255), primary_key=True)
    target = Column(String(255))

    def toJSON(self):
        return {'key': self.key, 'target': self.target}


## This definition is copied directly from: https://github.com/adsabs/metrics_service/blob/master/service/models.py
## We need to have it when we are sending/writing data into the metrics database
class MetricsModel(MetricsBase):
    __tablename__ = 'metrics'
    __bind_key__ = 'metrics'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String, nullable=False, index=True, unique=True)

    an_citations = Column(postgresql.REAL)
    an_refereed_citations = Column(postgresql.REAL)
    author_num = Column(Integer, default=1, server_default=text("1::integer"))
    citations = Column(postgresql.ARRAY(String), default=[], server_default=text("(ARRAY[]::varchar[])"))
    citation_num = Column(Integer, default=0)
    downloads = Column(postgresql.ARRAY(Integer), default=[])
    reads = Column(postgresql.ARRAY(Integer), default=[])
    refereed = Column(Boolean, default=False)
    refereed_citations = Column(postgresql.ARRAY(String), default=[])
    refereed_citation_num = Column(Integer, default=0)
    reference_num = Column(Integer, default=0)
    rn_citations = Column(postgresql.REAL)
    rn_citation_data = Column(postgresql.JSON)
    modtime = Column(DateTime)

    def toJSON(self):
        return dict(id=self.id,
                    bibcode=self.bibcode,
                    an_citations=self.an_citations,
                    an_refereed_citations=self.an_refereed_citations,
                    author_num=self.author_num,
                    citations=self.citations,
                    citation_num=self.citation_num,
                    downloads=self.downloads,
                    reads=self.reads,
                    refereed=self.refereed,
                    refereed_citations=self.refereed_citations,
                    refereed_citation_num=self.refereed_citation_num,
                    reference_num=self.reference_num,
                    rn_citations=self.rn_citations,
                    rn_citation_data=self.rn_citation_data,
                    modtime=self.modtime and get_date(self.modtime).isoformat() or None)
