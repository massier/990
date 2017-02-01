import dp_util.gen_util as gen_util
import dp_util.schema_util as schema_util
import dp_util.filing_util as filing_util
import dp_util.batch_insert as batch_insert
import logging

# adds into queue all available, electronic items not already in queue
def dump_index_into_queue(connection, limit = None):
    index_cursor = connection.cursor()
    queue_cursor = connection.cursor()
    index_query = "SELECT * FROM irs.form_index WHERE IFNULL(isAvailable, 0) > 0 AND IFNULL(isElectronic, 0) > 0 AND queueId IS NULL"
    if limit is not None:
        index_query += " LIMIT " + str(limit)
    index_cursor.execute(index_query)
    index = index_cursor.fetchall()
    queue_query = "INSERT IGNORE INTO irs.form_queue (dateAdded) VALUES (%s)"
    index_update_query = "UPDATE irs.form_index SET queueId = %s WHERE id = %s"

    for row in index:
        curr_time = gen_util.make_datestamp()
        try:
            if queue_cursor.execute(queue_query, (curr_time,)):
                queue_id = queue_cursor.lastrowid
                index_cursor.execute(index_update_query, (queue_id, row['id']))
        except:
            print("Error in queue filling")
            connection.rollback()
        else:
            connection.commit()


def load_queue_by_priority(connection, limit = None, priorities_selected = None):
    index_cursor = connection.cursor()
    queue_cursor = connection.cursor()

    index_query = "SELECT form_index.*, priorities.priority_id AS priority_id FROM irs.form_index AS form_index INNER JOIN irs.priorities AS priorities ON priorities.entry_id = form_index.id WHERE IFNULL(isAvailable, 0) > 0 AND IFNULL(isElectronic, 0) > 0 AND queueId IS NULL"
    if priorities_selected is not None:
        index_query += " AND priority_id IN ("
        for idx in priorities_selected:
            index_query = index_query + str(idx) + ", "
        index_query = index_query[:-2] + ")"
    if limit is not None:
        index_query += " LIMIT " + str(limit)
    index_cursor.execute(index_query)
    index = index_cursor.fetchall()


    queue_query = "INSERT IGNORE INTO irs.form_queue (dateAdded) VALUES (%s)"
    index_update_query = "UPDATE irs.form_index SET queueId = %s WHERE id = %s"

    for row in index:
        curr_time = gen_util.make_datestamp()
        try:
            if queue_cursor.execute(queue_query, (curr_time,)):
                queue_id = queue_cursor.lastrowid
                index_cursor.execute(index_update_query, (queue_id, row['id']))
        except:
            print("Error in queue filling")
            connection.rollback()
        else:
            connection.commit()


def load_queue_random(connection, limit = 5000):
    index_cursor = connection.cursor()
    index_query = "SELECT form_index.* FROM irs.form_index AS form_index WHERE IFNULL(isAvailable, 0) > 0 AND IFNULL (isElectronic, 0) > 0 AND queueId IS NULL ORDER BY RAND() LIMIT " + str(limit)
    index_cursor.execute(index_query)
    index = index_cursor.fetchall()
    queue_cursor = connection.cursor()
    queue_query = "INSERT IGNORE INTO irs.form_queue (dateAdded) VALUES (%s)"
    index_update_query = "UPDATE irs.form_index SET queueId = %s WHERE id = %s"

    for row in index:
        curr_time = gen_util.make_datestamp()
        try:
            if queue_cursor.execute(queue_query, (curr_time,)):
                queue_id = queue_cursor.lastrowid
                index_cursor.execute(index_update_query, (queue_id, row['id']))
        except:
            print("Error in queue filling")
            connection.rollback()
        else:
            connection.commit()


def have_schema(schema_list, version_year, version_num, form_type):
    for schema in schema_list:
        if version_year == schema['year'] and version_num == schema['version'] and form_type == schema['form']:
            return True
    return False


def debug_index_cursor(cursor, query, vals = None):
    if vals:
        cursor.execute(query, vals)
    else:
        cursor.execute(query)


def debug_queue_cursor(cursor, query, vals = None):
    if vals:
        cursor.execute(query, vals)
    else:
        cursor.execute(query)


def process_queue(client, connection, no_upload = False, batch_size = 20, priority_filter = None, rerun_failed = False):
    logger = logging.getLogger('main_logger')
    logger.info("Starting process_queue")
    available_schema = schema_util.get_available_schema(connection)
    schema_dict = schema_util.preload_schema(connection, available_schema)
    batch = batch_insert.BatchInsert() if batch_size > 1 else None
    queue_cursor = connection.cursor()
    if priority_filter is None:
        queue_query = "SELECT q.*, idx.* FROM irs.form_index idx JOIN irs.form_queue q ON q.id = idx.queueId WHERE q.success "
        if rerun_failed:
            queue_query += "= 0"
        else:
            queue_query += "IS NULL"
    else:
        queue_query = "SELECT q.*, idx.*, MAX(groups.rank) AS max_rank FROM irs.form_index idx JOIN irs.form_queue q ON q.id = idx.queueId LEFT JOIN irs.priorities priorities ON priorities.entry_id = idx.id INNER JOIN irs.priority_groups groups ON groups.id = priorities.priority_id WHERE q.success "
        if rerun_failed:
            queue_query += "= 0"
        else:
            queue_query += "IS NULL"
        queue_query += " AND priorities.priority_id IN ("
        for p in priority_filter:
            queue_query += str(p) + ", "
        queue_query = queue_query[:-2] + ") GROUP BY idx.id ORDER BY max_rank DESC"

    queue_cursor.execute(queue_query)
    queue_block = queue_cursor.fetchall()
    queue_cursor.close()
    queue_update_cursor = connection.cursor()
    queue_update_query = "UPDATE irs.form_queue SET dateProcessed = %s, name = %s, success = %s WHERE id = %s"
    idx_update_cursor = connection.cursor()
    idx_update_query = "UPDATE irs.form_index SET year = %s, version = %s WHERE queueId = %s"
    counter = 0
    batch_cursor = connection.cursor()
    success_dict = {}
    for row in queue_block:
        if row['version'] is not None:
            if not have_schema(available_schema, row['year'], row['version'], row['formType']):
                continue
        filing_util.download_filing(client, row['url'])
        tree = filing_util.load_xml(filing_util.filename_from_url(row['url']))
        version_year, version_num = filing_util.get_version(tree)
        form_type = row['formType']
        can_process = have_schema(available_schema, version_year, version_num, form_type)
        if not no_upload:
            debug_index_cursor(idx_update_cursor, idx_update_query, (version_year, version_num, row['queueId']))
            connection.commit()
        success = None
        if can_process:
            success = filing_util.process_xml(tree, connection, row['idx.id'], form_type, no_upload, batch)
        if not batch:
            timestamp = None if success is None else gen_util.make_datestamp()
            if not no_upload:
                queue_update_cursor.execute(queue_update_query, (timestamp, row['url'], success, row['queueId']))
                connection.commit()
        else:
            success_dict[row['queueId']] = (success, row['url'])
            counter += 1
            if (counter % batch_size == 0) or (row == queue_block[-1]):
                print(str(counter))
                batch_success = batch.execute(connection, batch_cursor, no_upload)
                # update queue
                for item_id, item_success in success_dict.items():
                    general_success = None if item_success[0] is None else (item_success[0] and batch_success)
                    timestamp = None if general_success is None else gen_util.make_datestamp()
                    if not no_upload:
                        debug_queue_cursor(queue_update_cursor, queue_update_query, (timestamp, item_success[1], general_success, item_id))
                connection.commit()
                success_dict.clear()

        filing_util.delete_filing(filing_util.filename_from_url(row['url']))
    if batch and not no_upload:
        batch_success = batch.execute(connection, batch_cursor, no_upload)
        # update queue
        for item_id, item_success in success_dict.items():
            general_success = None if item_success[0] is None else (item_success[0] and batch_success)
            timestamp = None if general_success is None else gen_util.make_datestamp()
            if not no_upload:
                debug_queue_cursor(queue_update_cursor, queue_update_query,
                                   (timestamp, item_success[1], general_success, item_id))
        connection.commit()
        success_dict.clear()
    logger.info("Exiting process queue")






