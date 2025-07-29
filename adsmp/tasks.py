
from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from adsmp import templates
from kombu import Queue
from adsmsg.msg import Msg
from sqlalchemy import create_engine, MetaData, Table, exc
from sqlalchemy.orm import sessionmaker
from adsmp.models import SitemapInfo
from collections import defaultdict
import pdb
# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSMasterPipelineCelery('master-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('index-records', app.exchange, routing_key='index-records'),
    Queue('rebuild-index', app.exchange, routing_key='rebuild-index'),
    Queue('delete-records', app.exchange, routing_key='delete-records'),
    Queue('index-solr', app.exchange, routing_key='index-solr'),
    Queue('index-metrics', app.exchange, routing_key='index-metrics'),
    Queue('index-data-links-resolver', app.exchange, routing_key='index-data-links-resolver'),
    Queue('generate-sitemap', app.exchange, routing_key='generate-sitemap'),
    Queue('generate-single-sitemap', app.exchange, routing_key='generate-single-sitemap'),
)


# ============================= TASKS ============================================= #

@app.task(queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.

    @param msg: protobuff that contains at minimum
        - bibcode
        - and specific payload
    """
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    type = app.get_msg_type(msg)
    bibcodes = []

    if status == 'deleted':
        if type == 'metadata':
            task_delete_documents(msg.bibcode)
        elif type == 'nonbib_records':
            for m in msg.nonbib_records: # TODO: this is very ugly, we are repeating ourselves...
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        else:
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, None)
            if record:
                logger.debug('Deleted %s, result: %s', type, record)

    elif status == 'active':
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'augment':
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, 'augment',
                                        msg.toJSON(including_default_value_fields=True))
            if record:
                logger.debug('Saved augment message: %s', msg)

        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            if record:
                logger.debug('Saved record: %s', record)
            if type == 'metadata':
                # with new bib data we request to augment the affiliation
                # that pipeline will eventually respond with a msg to task_update_record
                logger.debug('requesting affilation augmentation for %s', msg.bibcode)
                app.request_aff_augment(msg.bibcode)

    else:
        logger.error('Received a message with unclear status: %s', msg)


@app.task(queue='rebuild-index')
def task_rebuild_index(bibcodes, solr_targets=None):
    """part of feature that rebuilds the entire solr index from scratch

    note that which collection to update is part of the url in solr_targets
    """
    reindex_records(bibcodes, force=True, update_solr=True, update_metrics=False, update_links=False, commit=False,
                    ignore_checksums=True, solr_targets=solr_targets, update_processed=False, priority=0)


@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                       ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """
    Sends data to production systems: solr, metrics and resolver links
    Only does send if data has changed
    This task is (normally) called by the cronjob task
    (that one, quite obviously, is in turn started by cron)
    Use code also called by task_rebuild_index,
    """
    reindex_records(bibcodes, force=force, update_solr=update_solr, update_metrics=update_metrics, update_links=update_links, commit=commit,
                    ignore_checksums=ignore_checksums, solr_targets=solr_targets, update_processed=update_processed)


@app.task(queue='index-solr')
def task_index_solr(solr_records, solr_records_checksum, commit=False, solr_targets=None, update_processed=True):
    app.index_solr(solr_records, solr_records_checksum, solr_targets, commit=commit, update_processed=update_processed)


@app.task(queue='index-metrics')
def task_index_metrics(metrics_records, metrics_records_checksum, update_processed=True):
    # todo: create insert and update lists before queuing?
    app.index_metrics(metrics_records, metrics_records_checksum)


@app.task(queue='index-data-links-resolver')
def task_index_data_links_resolver(links_data_records, links_data_records_checksum, update_processed=True):
    app.index_datalinks(links_data_records, links_data_records_checksum, update_processed=update_processed)


def reindex_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                    ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """Receives bibcodes that need production store updated
    Receives bibcodes and checks the database if we have all the
    necessary pieces to push to production store. If not, then postpone and
    send later.
    we consider a record to be ready for solr if these pieces were updated
    (and were updated later than the last 'processed' timestamp):
        - bib_data
        - nonbib_data
        - orcid_claims
    if the force flag is true only bib_data is needed
    for solr, 'fulltext' is not considered essential; but updates to fulltext will
    trigger a solr_update (so it might happen that a document will get
    indexed twice; first with only metadata and later on incl fulltext)
    """

    if isinstance(bibcodes, basestring):
        bibcodes = [bibcodes]

    if not (update_solr or update_metrics or update_links):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running index-records for: %s', bibcodes)
    solr_records = []
    metrics_records = []
    links_data_records = []
    solr_records_checksum = []
    metrics_records_checksum = []
    links_data_records_checksum = []
    links_url = app.conf.get('LINKS_RESOLVER_UPDATE_URL')

    if update_solr:
        fields = None  # Load all the fields since solr records grab data from almost everywhere
    else:
        # Optimization: load only fields that will be used
        fields = ['bibcode', 'augments_updated', 'bib_data_updated', 'fulltext_updated', 'metrics_updated', 'nonbib_data_updated', 'orcid_claims_updated', 'processed']
        if update_metrics:
            fields += ['metrics', 'metrics_checksum']
        if update_links:
            fields += ['nonbib_data', 'bib_data', 'datalinks_checksum']

    # check if we have complete record
    for bibcode in bibcodes:
        r = app.get_record(bibcode, load_only=fields)

        if r is None:
            logger.error('The bibcode %s doesn\'t exist!', bibcode)
            continue

        augments_updated = r.get('augments_updated', None)
        bib_data_updated = r.get('bib_data_updated', None)
        fulltext_updated = r.get('fulltext_updated', None)
        metrics_updated = r.get('metrics_updated', None)
        nonbib_data_updated = r.get('nonbib_data_updated', None)
        orcid_claims_updated = r.get('orcid_claims_updated', None)

        year_zero = '1972'
        processed = r.get('processed', adsputils.get_date(year_zero))
        if processed is None:
            processed = adsputils.get_date(year_zero)

        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])

        if is_complete or (force is True and bib_data_updated):
            if force is False and all([
                    augments_updated and augments_updated < processed,
                    bib_data_updated and bib_data_updated < processed,
                    nonbib_data_updated and nonbib_data_updated < processed,
                    orcid_claims_updated and orcid_claims_updated < processed
                   ]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            if force:
                logger.debug('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
            # build the solr record
            if update_solr:
                solr_payload = solr_updater.transform_json_record(r)
                # ADS microservices assume the identifier field exists and contains the canonical bibcode:
                if 'identifier' not in solr_payload:
                    solr_payload['identifier'] = []
                if 'bibcode' in solr_payload and solr_payload['bibcode'] not in solr_payload['identifier']:
                    solr_payload['identifier'].append(solr_payload['bibcode'])
                logger.debug('Built SOLR: %s', solr_payload)
                solr_checksum = app.checksum(solr_payload)
                if ignore_checksums or r.get('solr_checksum', None) != solr_checksum:
                    solr_records.append(solr_payload)
                    solr_records_checksum.append(solr_checksum)
                else:
                    logger.debug('Checksum identical, skipping solr update for: %s', bibcode)

            # get data for metrics
            if update_metrics:
                metrics_payload = r.get('metrics', None)
                metrics_checksum = app.checksum(metrics_payload or '')
                if (metrics_payload and ignore_checksums) or (metrics_payload and r.get('metrics_checksum', None) != metrics_checksum):
                    metrics_payload['bibcode'] = bibcode
                    logger.debug('Got metrics: %s', metrics_payload)
                    metrics_records.append(metrics_payload)
                    metrics_records_checksum.append(metrics_checksum)
                else:
                    logger.debug('Checksum identical or no metrics data available, skipping metrics update for: %s', bibcode)

            if update_links and links_url:
                datalinks_payload = app.generate_links_for_resolver(r)
                if datalinks_payload:
                    datalinks_checksum = app.checksum(datalinks_payload)
                    if ignore_checksums or r.get('datalinks_checksum', None) != datalinks_checksum:
                        links_data_records.append(datalinks_payload)
                        links_data_records_checksum.append(datalinks_checksum)
        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
    if solr_records:
        task_index_solr.apply_async(
            args=(solr_records, solr_records_checksum,),
            kwargs={
               'commit': commit,
               'solr_targets': solr_targets,
               'update_processed': update_processed
            }
        )
    if metrics_records:
        task_index_metrics.apply_async(
            args=(metrics_records, metrics_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )
    if links_data_records:
        task_index_data_links_resolver.apply_async(
            args=(links_data_records, links_data_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )


@app.task(queue='delete-records')
def task_delete_documents(bibcode):
    """Delete document from SOLR and from our storage.
    @param bibcode: string
    """
    logger.debug('To delete: %s', bibcode)
    app.delete_by_bibcode(bibcode)
    deleted, failed = solr_updater.delete_by_bibcodes([bibcode], app.conf['SOLR_URLS'])
    if len(failed):
        logger.error('Failed deleting documents from solr: %s', failed)
    if len(deleted):
        logger.debug('Deleted SOLR docs: %s', deleted)

    if app.metrics_delete_by_bibcode(bibcode):
        logger.debug('Deleted metrics record: %s', bibcode)
    else:
        logger.debug('Failed to deleted metrics record: %s', bibcode)

@app.task(queue='populate-sitemap-table') 
def task_populate_sitemap_table(bibcodes, action):
    """
    Populate the sitemap table for the given bibcodes
    """

    sitemap_dir = app.sitemap_dir

    if action == 'delete-table':
        # reset and empty all entries in sitemap table
        app.delete_contents(SitemapInfo)

        # move all sitemap files to a backup directory
        app.backup_sitemap_files(sitemap_dir)
        return
    
    # TODO: Implement 'remove' action
    elif action == 'remove': 
        #TODO: how to handle empty files in case of mass deletion?
        pass 

    elif action in ['add', 'force-update']:
        if isinstance(bibcodes, basestring):
            bibcodes = [bibcodes]

        logger.debug('Updating sitemap info for: %s', bibcodes)
        fields = ['id', 'bibcode', 'bib_data_updated']
        sitemap_records = []

        # update all record_id from records table into sitemap table
        successful_count = 0
        failed_count = 0
        
        for bibcode in bibcodes:
            try:
                record = app.get_record(bibcode, load_only=fields)
                sitemap_info = app.get_sitemap_info(bibcode) 

                if record is None:
                    logger.error('The bibcode %s doesn\'t exist!', bibcode)
                    failed_count += 1
                    continue

                # Create sitemap record data structure with default None values for filename_lastmoddate and sitemap_filename
                sitemap_record = {
                        'record_id': record.get('id'), # Records object uses attribute access
                        'bibcode': record.get('bibcode'), # Records object uses attribute access  
                        'bib_data_updated': record.get('bib_data_updated', None),
                        'filename_lastmoddate': None, 
                        'sitemap_filename': None,
                        'update_flag': False
                    }
                
                # New sitemap record
                if sitemap_info is None:            
                    sitemap_record['update_flag'] = True
                    sitemap_records.append((sitemap_record['record_id'], sitemap_record['bibcode']))
                    app.populate_sitemap_table(sitemap_record) 
                    

                else:
                    # Sitemap record exists, update it 
                    sitemap_record['filename_lastmoddate'] = sitemap_info.get('filename_lastmoddate', None)
                    sitemap_record['sitemap_filename'] = sitemap_info.get('sitemap_filename', None)

                    bib_data_updated = sitemap_record.get('bib_data_updated', None) 
                    file_modified = sitemap_record.get('filename_lastmoddate', None)

                    # If action is 'add' and bibdata was updated, or if action is 'force-update', set update_flag to True
                    # Sitemap files will need to be updated in task_update_sitemap_files
                    if action == 'force-update':
                        sitemap_record['update_flag'] = True
                    elif action == 'add':
                        # Sitemap file has never been generated OR data updated since last generation
                        if file_modified is None or (file_modified and bib_data_updated and bib_data_updated > file_modified):
                            sitemap_record['update_flag'] = True
                    
                    app.populate_sitemap_table(sitemap_record, sitemap_info)
                
                successful_count += 1
                logger.debug('Successfully processed sitemap for bibcode: %s', bibcode)
                
            except Exception as e:
                failed_count += 1
                logger.error('Failed to populate sitemap table for bibcode %s: %s', bibcode, str(e))
                # Continue to next bibcode instead of crashing
                continue
        
        logger.info('Sitemap population completed: %d successful, %d failed out of %d total bibcodes', 
                    successful_count, failed_count, len(bibcodes)) 
        logger.info('%s Total sitemap records created: %s', len(sitemap_records), sitemap_records)

# @app.task(queue='generate-single-sitemap')
# def task_generate_single_sitemap(sitemap_filename, record_ids):
#     """Worker task: Generate a single sitemap file from the given record IDs"""
    
#     try:
#         with app.session_scope() as session:
#             # Get all records for this specific file
#             file_records = (
#                 session.query(SitemapInfo)
#                 .filter(SitemapInfo.id.in_(record_ids))
#                 .all()
#             )
            
#             if not file_records:
#                 logger.warning('No records found for sitemap file %s', sitemap_filename)
#                 return False
            
#             logger.debug('Processing sitemap file %s with %d records', sitemap_filename, len(file_records))
            
#             url_entries = []
            
#             for info in file_records:
#                 # Update database record
#                 info.filename_lastmoddate = adsputils.get_date()
#                 lastmod_date = info.bib_data_updated.date() if info.bib_data_updated else adsputils.get_date().date()
#                 # Use ADS URL pattern for tasks (could be made configurable)
#                 url_entry = templates.format_url_entry(info.bibcode, lastmod_date)
#                 url_entries.append(url_entry)
#                 info.update_flag = False
            
#             # Write the XML file
#             sitemap_content = templates.render_sitemap_file(''.join(url_entries))
#             with open(sitemap_filename, 'w', encoding='utf-8') as file:
#                 file.write(sitemap_content)
            
#             session.commit()
#             logger.debug('Successfully generated %s with %d records', sitemap_filename, len(url_entries))
#             return True
            
#     except Exception as e:
#         logger.error('Failed to generate sitemap file %s: %s', sitemap_filename, str(e))
#         return False

# @app.task(queue='update-sitemap-files') 
# def task_update_sitemap_files():
#     """Orchestrator task: Spawns parallel tasks for each sitemap file"""
    
#     try:
#         logger.info('Starting sitemap file generation')
        
#         # Find files that need updating
#         with app.session_scope() as session:
#             files_needing_update_subquery = (
#                 session.query(SitemapInfo.sitemap_filename.distinct())
#                 .filter(SitemapInfo.update_flag == True)
#             )
            
#             all_records = (
#                 session.query(SitemapInfo)
#                 .filter(SitemapInfo.sitemap_filename.in_(files_needing_update_subquery))
#                 .all()
#             )
            
#             if not all_records:
#                 logger.info('No sitemap files need updating')
#                 return
            
#             logger.info('Found %d records to process in sitemap files', len(all_records))
            
#             # Group records by filename
#             files_dict = defaultdict(list)
#             for record in all_records:
#                 files_dict[record.sitemap_filename].append(record.id)  
        
#         # Spawn parallel Celery tasks - one per file
#         logger.info('Spawning %d parallel tasks for sitemap files', len(files_dict))
#         task_results = []
        
#         for sitemap_filename, record_ids in files_dict.items():
#             # Launch async task for this file
#             result = task_generate_single_sitemap.apply_async(
#                 args=(sitemap_filename, record_ids)
#             )
#             task_results.append((sitemap_filename, result))
#             logger.debug('Spawned task for %s with %d records', sitemap_filename, len(record_ids))
        
#         # Wait for all parallel tasks to complete
#         successful_files = 0
#         failed_files = 0
        
#         for sitemap_filename, result in task_results:
#             try:
#                 # Wait for this specific task to complete (with timeout)
#                 success = result.get(timeout=300)  # 5 minute timeout per file
#                 if success:
#                     successful_files += 1
#                     logger.debug('Successfully completed %s', sitemap_filename)
#                 else:
#                     failed_files += 1
#                     logger.error('Task returned False for %s', sitemap_filename)
#             except Exception as e:
#                 failed_files += 1
#                 logger.error('Task failed for %s: %s', sitemap_filename, str(e))
        
#         logger.info('Sitemap generation completed: %d successful, %d failed', 
#                    successful_files, failed_files)
        
#         # Generate index files after all individual files are done
#         logger.info('Generating sitemap index and robots.txt files')
#         app.create_robot_txt_file()
#         app.create_sitemap_index()
        
#     except Exception as e:
#         logger.error('Error in orchestrator task: %s', str(e))
#         raise

#         # TODO: add directory names : about, help, blog  
#         # TODO: need to query github to find when above dirs are updated: https://docs.github.com/en/rest?apiVersion=2022-11-28
#         # TODO: need to generate an API token from ADStailor (maybe)
#         # TODO: OTHER -- do this for ADS and SciX 

if __name__ == '__main__':
    app.start()
