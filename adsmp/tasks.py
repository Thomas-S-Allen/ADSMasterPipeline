
from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from kombu import Queue
from adsmsg.msg import Msg

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
        elif type == 'classify':
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, 'classify',
                                        msg.toJSON(including_default_value_fields=True))
            if record:
                logger.debug('Saved classify message: %s', msg)

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
                # We also want to request the classification of the document
                logger.debug('requesting classification for %s', msg.bibcode)
                app.request_classification(msg.bibcode)

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


if __name__ == '__main__':
    app.start()
