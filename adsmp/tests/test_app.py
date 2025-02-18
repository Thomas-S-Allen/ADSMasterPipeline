#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import unittest
import os
import sys
import copy
import json

import adsputils
from adsmp import app, models
from adsmp.models import Base, MetricsBase
from adsputils import get_date
import testing.postgresql
from sqlalchemy.exc import IntegrityError, SQLAlchemyError


class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    
    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15678, user='postgres', 
                                          database='test')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        MetricsBase.metadata.bind = self.app._metrics_engine
        MetricsBase.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        MetricsBase.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'sqlite:///'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'sqlite:///'

    def test_mark_processed(self):
        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='success')
        r = self.app.get_record('abc')
        self.assertEqual(r, None)
        
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='success')
        r = self.app.get_record('abc')
        
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['status'])

        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='solr-failed')
        r = self.app.get_record('abc')
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['processed'])
        self.assertEqual(r['status'], 'solr-failed')

    def test_index_solr(self):
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.update_storage('foo', 'bib_data', {'bibcode': 'foo', 'hey': 1})
        
        with mock.patch('adsmp.solr_updater.update_solr', return_value=[200]):
            self.app.index_solr([{'bibcode': 'abc'},
                                 {'bibcode': 'foo'}],
                                ['checksum1', 'checksum2'],
                                         ['http://solr1'])
            with self.app.session_scope() as session:
                for x in ['abc', 'foo']:
                    r = session.query(models.Records).filter_by(bibcode=x).first()
                    self.assertTrue(r.processed)
                    self.assertFalse(r.metrics_processed)
                    self.assertTrue(r.solr_processed)
                    
        # pretend group failure and then success when records sent individually
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503], [200], [200]]
            self.app.index_solr([{'bibcode': 'abc'},
                                 {'bibcode': 'foo'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            # self.assertTrue(len(failed) == 0)
            x = str(mp.call_args_list[0])
            self.assertTrue('abc' in x)
            self.assertTrue('success' in x)
            self.assertTrue('solr' in x)
            self.assertEqual(us.call_count, 3)
            x = str(mp.call_args_list[1])
            self.assertTrue('foo' in x)
            self.assertTrue('success' in x)
            self.assertTrue('solr' in x)

        # pretend failure and success without body
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503, 503], Exception('body failed'), 200, Exception('body failed'), 200]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'BAD BODY'},
                                 {'bibcode': 'foo', 'body': 'BAD BODY'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 5)
            # self.assertTrue(len(failed) == 0)
            self.assertEqual(mp.call_count, 2)
            x = str(us.call_args_list[-2])
            self.assertTrue('http://solr1' in x)
            self.assertTrue('foo' in x)
            self.assertTrue('body' in x)
            self.assertTrue('BAD BODY' in x)
            x = str(us.call_args_list[-1])
            self.assertTrue('http://solr1' in x)
            self.assertTrue('foo' in x)

        # pretend failure and then lots more failure
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us:
            us.side_effect = [[503, 503],
                              Exception('body failed'), Exception('body failed'),
                              Exception('body failed'), Exception('body failed')]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'bad body'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 5)

        # pretend failure and and then failure for a mix of reasons
        with mock.patch('adsmp.solr_updater.update_solr') as us:
            us.side_effect = [[503, 503], Exception('body failed'), Exception('failed'), Exception('failed')]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'good body'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            if sys.version_info > (3,):
                call_dict = "{'bibcode': 'foo', 'body': 'good body'}"
            else:
                call_dict = "{'body': 'good body', 'bibcode': 'foo'}"
            self.assertEqual(str(us.call_args_list[-1]), "call([%s], ['http://solr1'], commit=False, ignore_errors=False)" % call_dict)

        # pretend failure and and then a mix of failure and success
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503, 503], Exception('body failed'), [200]]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'good body'}],
                                ['checksum1', 'checksum2'],
                                         ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            # self.assertTrue(len(failed) == 1)
            self.assertEqual(us.call_count, 4)
            self.assertEqual(mp.call_count, 2)
            x = str(us.call_args_list[-1])
            self.assertTrue('foo' in x)
            self.assertTrue('good body' in x)
            self.assertTrue('http://solr1' in x)

    def test_update_metrics(self):
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        self.app.update_storage('foo', 'metrics', {
                    'bibcode': 'foo',
                    'citation_num': 6,
                    'author_num': 3,
                    })
        
        batch_metrics = [self.app.get_record('abc')['metrics'], self.app.get_record('foo')['metrics']]
        batch_checksum = ['checksum1', 'checksum2']
        self.app.index_metrics(batch_metrics, batch_checksum)
        
        for x in ['abc', 'foo']:
            r = self.app.get_record(x)
            self.assertTrue(r['processed'])
            self.assertTrue(r['metrics_processed'])
            self.assertFalse(r['solr_processed'])
            
    def test_delete_metrics(self):
        """Makes sure we can delete a metrics record by bibcode"""
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        r = self.app.get_record('abc')
        self.app.index_metrics([r], ['checksum'])
        m = self.app.get_metrics('abc')
        self.assertTrue(m, 'intialized metrics data')
        self.app.metrics_delete_by_bibcode('abc')
        m = self.app.get_metrics('abc')
        self.assertFalse(m, 'deleted metrics data')
        
    def test_update_records(self):
        """Makes sure we can write recs into the storage."""
        now = adsputils.get_date()
        last_time = adsputils.get_date()
        for k in ['bib_data', 'nonbib_data', 'orcid_claims']:
            self.app.update_storage('abc', k, {'foo': 'bar', 'hey': 1})
            with self.app.session_scope() as session:
                r = session.query(models.Records).filter_by(bibcode='abc').first()
                self.assertTrue(r.id == 1)
                self.assertTrue(r.scix_id == 'scix:0000-0000-0011')
                j = r.toJSON()
                self.assertEqual(j[k], {'foo': 'bar', 'hey': 1})
                t = j[k + '_updated']
                self.assertTrue(now < t)
                self.assertTrue(last_time < j['updated'])
                last_time = j['updated']
        
        self.app.update_storage('abc', 'fulltext', {'body': 'foo bar'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertTrue(r.id == 1)
            self.assertTrue(r.scix_id == 'scix:0000-0000-0011')
            j = r.toJSON()
            self.assertEqual(j['fulltext'], {'body': 'foo bar'})
            t = j['fulltext_updated']
            self.assertTrue(now < t)
        
        r = self.app.get_record('abc')
        self.assertEqual(r['id'], 1)
        self.assertEqual(r['scix_id'],'scix:0000-0000-0011')
        self.assertEqual(r['processed'], None)
        
        r = self.app.get_record(['abc'])
        self.assertEqual(r[0]['id'], 1)
        self.assertEqual(r[0]['scix_id'],'scix:0000-0000-0011')
        self.assertEqual(r[0]['processed'], None)
        
        r = self.app.get_record('abc', load_only=['id'])
        self.assertEqual(r['id'], 1)
        self.assertFalse('processed' in r)

        with self.assertRaises(ValueError) as e:
            self.app.mark_processed(['abc'], 'foobar')
            self.assertTrue('foobar' in e.exception)
        
        # now delete it
        self.app.delete_by_bibcode('abc')
        r = self.app.get_record('abc')
        self.assertTrue(r is None)
        with self.app.session_scope() as session:
            r = session.query(models.ChangeLog).filter_by(key='bibcode:abc').first()
            self.assertTrue(r.key, 'abc')

    def test_index_metrics_database_failure(self):
        """
           verify handles failure from database
           send one bibcode, verify there are two commits
        """
        self.app.update_storage('abc', 'metrics', {
            'author_num': 1,
            'bibcode': 'abc',
        })

        trans = mock.Mock()
        trans.commit.side_effect = SQLAlchemyError('test')
        m = mock.Mock()
        m.begin_nested.return_value = trans
        m.__exit__ = mock.Mock()
        m.__enter__ = mock.Mock()
        m.__enter__.return_value = mock.Mock()
        m.__enter__.return_value.begin_nested.return_value = trans
        # init database so timestamps and checksum can be updated
        with mock.patch('adsmp.app.ADSMasterPipelineCelery.metrics_session_scope', return_value=m) as p:
            metrics_payload = {'bibcode': 'abc', 'author_num': 1}
            checksum = 'checksum'
            self.app.index_metrics([metrics_payload], [checksum])
            self.assertEqual(trans.commit.call_count, 2)

    def test_index_datalinks_success(self):
        """verify passed data sent to resolver service
           verify handles success from service
           verify records table updated with processed, status and checksum
        """
        m = mock.Mock()
        m.status_code = 200
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            self.assertEqual(p.call_count, 1)
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], 'thechecksum')
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'success')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_service_failure(self):
        """
           verify handles failure from service
        """
        m = mock.Mock()
        m.status_code = 500
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})

            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(p.call_count, 2)
            self.assertEqual(rec['datalinks_checksum'], None)
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'links-failed')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_service_only_batch_failure(self):
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put') as p:
            bad = mock.Mock()
            bad.status_code = 500
            good = mock.Mock()
            good.status_code = 200
            p.side_effect = [bad, good]
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            self.assertEqual(p.call_count, 2)
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], 'thechecksum')
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'success')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_update_processed_false(self):
        m = mock.Mock()
        m.status_code = 200
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum], update_processed=False)
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], None)
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], None)
            self.assertEqual(rec['datalinks_processed'], None)

    def test_update_records_db_error(self):
        """test database exception IntegrityError is caught"""
        with mock.patch('sqlalchemy.orm.session.Session.commit', side_effect=[IntegrityError('a', 'b', 'c', 'd'), None]):
            self.assertRaises(IntegrityError, self.app.update_storage, 'abc', 'nonbib_data', '{}')
        
    def test_rename_bibcode(self):
        self.app.update_storage('abc', 'metadata', {'foo': 'bar', 'hey': 1})
        r = self.app.get_record('abc')
        
        self.app.rename_bibcode('abc', 'def')
        
        with self.app.session_scope() as session:
            ref = session.query(models.IdentifierMapping).filter_by(key='abc').first()
            self.assertTrue(ref.target, 'def')
            
        self.assertTrue(self.app.get_changelog('abc'), [{'target': u'def', 'key': u'abc'}])

    def test_generate_links_for_resolver(self):
        only_nonbib = {'bibcode': 'asdf',
                       'nonbib_data': 
                       {'data_links_rows': [{'url': ['http://arxiv.org/abs/1902.09522']}]}}
        links = self.app.generate_links_for_resolver(only_nonbib)
        self.assertEqual(only_nonbib['bibcode'], links['bibcode'])
        self.assertEqual(only_nonbib['nonbib_data']['data_links_rows'], links['data_links_rows'])

        only_bib = {'bibcode': 'asdf',
                    'bib_data':
                    {'links_data': ['{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522"}']}}
        links = self.app.generate_links_for_resolver(only_bib)
        self.assertEqual(only_bib['bibcode'], links['bibcode'])
        first = links['data_links_rows'][0]
        self.assertEqual('http://arxiv.org/abs/1902.09522', first['url'][0])
        self.assertEqual('ESOURCE', first['link_type'])
        self.assertEqual('EPRINT_HTML', first['link_sub_type'])
        self.assertEqual([''], first['title'])
        self.assertEqual(0, first['item_count'])

        bib_and_nonbib = {'bibcode': 'asdf',
                          'bib_data':
                          {'links_data': ['{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522zz"}']},
                          'nonbib_data':
                          {'data_links_rows': [{'url': ['http://arxiv.org/abs/1902.09522']}]}}
        links = self.app.generate_links_for_resolver(bib_and_nonbib)
        self.assertEqual(only_nonbib['bibcode'], links['bibcode'])
        self.assertEqual(only_nonbib['nonbib_data']['data_links_rows'], links['data_links_rows'])

        # string in database
        only_bib = {'bibcode': 'asdf',
                    'bib_data':
                    {'links_data': [u'{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522"}']}}
        links = self.app.generate_links_for_resolver(only_bib)
        self.assertEqual(only_bib['bibcode'], links['bibcode'])
        first = links['data_links_rows'][0]
        self.assertEqual('http://arxiv.org/abs/1902.09522', first['url'][0])
        self.assertEqual('ESOURCE', first['link_type'])
        self.assertEqual('EPRINT_HTML', first['link_sub_type'])
        
        # bad string in database
        with mock.patch.object(self.app.logger, 'error') as m:
            only_bib = {'bibcode': 'testbib',
                        'bib_data':
                        {'links_data': u'foobar[!)'}}
            links = self.app.generate_links_for_resolver(only_bib)
            self.assertEqual(None, links)
            self.assertEqual(1, m.call_count)
            m_args = m.call_args_list
            self.assertTrue('testbib' in str(m_args[0]))
            self.assertTrue('foobar' in str(m_args[0]))


if __name__ == '__main__':
    unittest.main()
