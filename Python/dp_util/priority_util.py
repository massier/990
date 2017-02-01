import warnings

def list_priorities(connection):
    priority_cursor = connection.cursor()
    priority_query = "SELECT `id`, `name`, `description`, `rank` FROM irs.priority_groups ORDER BY `id` ASC"
    priority_cursor.execute(priority_query)
    return priority_cursor.fetchall()


def collect_priority_stats(connection, priority_id):
    priority_cursor = connection.cursor()
    priority_query = "SELECT groups.id AS group_id, COUNT(*) AS group_count FROM irs.priorities AS priorities JOIN irs.priority_groups AS groups ON priorities.priority_id = groups.id INNER JOIN irs.form_index AS form_index ON priorities.entry_id = form_index.id GROUP BY group_id"
    priority_cursor.execute(priority_query)
    return priority_cursor.fetchall()


def load_ein_list(file_name):
    pass

def add_priority_assignments(connection, in_table, priorities):
    insert_cursor = connection.cursor()
    insert_query = "INSERT IGNORE INTO irs.priorities (entry_id, priority_id) SELECT form_index.id AS entry_id, %s AS priority_id, SUBSTRING(form_index.taxPeriod, 1, 4) AS taxYear FROM irs.`" + in_table + "` as source_table INNER JOIN irs.form_index as form_index ON source_table._____ = form_index.ein WHERE taxYear LIKE source_table.___"

    success = True
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for priority_id in priorities:
                insert_cursor.execute(insert_query, (priority_id,))
            for w_elem in w:
                success = False
                print(str(w_elem.message))
    except Exception as e:
        print("Canceling due to exception:")
        print(e)
        success = False
    if success:
        print("List updated")
        connection.commit()
    else:
        print("Rolling back")
        connection.rollback()

