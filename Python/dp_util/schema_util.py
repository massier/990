from lxml import etree as ET
import dp_util.db_util as db_util
import os
import dp_util.php_util as php_util


def alter_tables(connection):
    schema_cursor = connection.cursor()
    schema_query = "SELECT * FROM irs.schema WHERE !ISNULL(schema.dataType)"
    schema_cursor.execute(schema_query)
    table_dict = {}
    for itm in schema_cursor:
        if itm['tableName'] in table_dict:
            table_dict[itm['tableName']].append(itm)
        else:
            table_dict[itm['tableName']] = [itm]

    for table, fields in table_dict.items():
        alter_query = ("ALTER TABLE `" + table + "` ")
        handled_rows = []
        altered = False
        for row in fields:
            if not row["fieldName"] in handled_rows:
                handled_rows.append(row["fieldName"])

                if row["dataType"] == 'Decimal':
                    altered = True
                    alter_query += "MODIFY `" + row["fieldName"] + "` "
                    alter_query += "decimal(24,8) DEFAULT NULL, "

        if altered:
            schema_cursor.execute(alter_query[:-2])
            connection.commit()
    print("Done")


def populate_tables_from_schema(connection):
    schema_cursor = connection.cursor()
    schema_query = "SELECT * FROM irs.schema WHERE !ISNULL(schema.dataType)"
    schema_cursor.execute(schema_query)
    table_dict = {}
    for itm in schema_cursor:
        if itm['tableName'] in table_dict:
            table_dict[itm['tableName']].append(itm)
        else:
            table_dict[itm['tableName']] = [itm]

    for table, fields in table_dict.items():
        drop_query = "DROP TABLE IF EXISTS `"+table+"`"
        schema_cursor.execute(drop_query)
        connection.commit()
        create_query = ("CREATE TABLE `"+table+"` ("
                       "`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                       "`fk` int(10) unsigned DEFAULT NULL,"
                        "`row` int(10) unsigned DEFAULT NULL")
        handled_rows = []
        for row in fields:
            if not row["fieldName"] in handled_rows:
                handled_rows.append(row["fieldName"])
                create_query += " `" + row["fieldName"] + "` "
                if row["dataType"] == 'Decimal':
                    create_query += "decimal(24,8) DEFAULT NULL,"
                if row["dataType"] == 'DateTime':
                    create_query += "datetime DEFAULT NULL,"
                if row["dataType"] == 'Boolean':
                    create_query += "tinyint(4) DEFAULT NULL,"
                if row["dataType"] == 'String':
                    max_len = row["maxLength"] if row["maxLength"] is not None else 255
                    create_query += "varchar(" + str(max_len) + ") DEFAULT NULL,"
                if row["dataType"] == 'String[]':
                    create_query += "varchar(255) DEFAULT NULL,"

        create_query += " PRIMARY KEY (`id`) )"
        schema_cursor.execute(create_query)
        connection.commit()
        index_query = "ALTER TABLE `" + table + "` ADD INDEX `fk` (`fk`)"
        schema_cursor.execute(index_query)
        connection.commit()
    print("Done")


def get_available_schema(connection):
    schema_cursor = connection.cursor()
    schema_query = "SELECT form, year, version FROM irs.schema GROUP BY form, year, version"
    schema_cursor.execute(schema_query)
    schema_types = schema_cursor.fetchall()
    schema_cursor.close()
    return schema_types


def clear_existing_tables(connection):
    db_cursor = connection.cursor()
    db_query = "SHOW TABLES IN irs WHERE Tables_in_irs NOT RLIKE 'dupes*|form_index*|schema*|priority_groups|LeadingAge*|ARC|priorities'"
    db_cursor.execute(db_query)
    tables = db_cursor.fetchall()
    trunc_cursor = connection.cursor()

    for table in tables:
        trunc_query = "TRUNCATE TABLE irs.`" + table['Tables_in_irs'] + "`"
        trunc_cursor.execute(trunc_query)


def preload_schema(connection, schema_dict):

    pass


def resolve_element(connection, element, path, highest_level, type, year, version, tree_path):
    if element.get('schemaLocation'):
        resolve_element(connection, import_xml_fragment(path, element.get('schemaLocation')), os.path.dirname(join_rel_path(path, element.get('schemaLocation'))), highest_level, type, year, version, tree_path)
    else:
        if element.get('name'):
            schema_entry = {}
            schema_entry['year'] = year
            schema_entry['form'] = type
            schema_entry['version'] = version
            schema_entry['highestLevel'] = highest_level
            schema_entry['tableName'] = None # todo
            schema_entry['path'] = None # todo
            schema_entry['dataType'] = None # todo
            schema_entry['maxLength'] = None # todo
            schema_entry['minOccurs'] = None # todo
            schema_entry['description'] = None # todo
            schema_entry['pattern'] = None # todo
            schema_entry['itemList'] = None # todo
            #todo: fill in
            add_schema_entry(connection, schema_entry)
        for definition in element:
            resolve_element(connection, definition, path, highest_level, type, year, version)


def add_schema_entry(connection, record):
    pass
    #schema_cursor = connection.cursor()
    # todo: fill in


def join_rel_path(path, path2):
    joined_path = os.path.join(path, path2)
    normed_path = os.path.normpath(joined_path)
    return normed_path


def import_xml_fragment(path, path2):
    normed_path = join_rel_path(path, path2)
    if(os.path.isfile(normed_path)):
        frag = ET.parse(normed_path)
        return frag.getroot()
    else:
        print("Path not found")
        return None


def table_schema(connection, root_file, type, year, version):
    if not root_file:
        return
    schema = ET.parse(root_file)

    schema.xinclude()
    schema_root = schema.getroot()
    path = os.path.dirname(root_file)
    header = schema_root[1]
    data = schema_root[2]
    if header.get('schemaLocation'):
        header = import_xml_fragment(path, header.get('schemaLocation'))
    if data.get('schemaLocation'):
        data = import_xml_fragment(path, data.get('schemaLocation'))
    for definition in header:
        resolve_element(connection, definition, path, "ReturnHeader", type, year, version, definition['name'])
    for definition in data:
        resolve_element(connection, definition, path, "ReturnData", type, year, version, definition['name'])

    print("next...")


def table_schema_php(connection, root_file, type, year, version):
    if not root_file:
        return