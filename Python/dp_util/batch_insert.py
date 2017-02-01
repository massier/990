import dp_util.db_util as db_util
import logging
import warnings

class BatchInsert:

    def __init__(self):
        self.tables = {}
        self.table_columns = {}

    def add_table(self, table_name, table_def, connection):
        if table_name in self.tables:
            return
        self.tables[table_name] = []
        self.table_columns[table_name] = db_util.get_columns(table_name, connection)

    def execute(self, connection, cursor = None, no_upload = False):
        logger = logging.getLogger('main_logger')
        logger.info("Beginning batch execution")
        if len(self.tables) < 1:
            return
        if cursor is None:
            cursor = connection.cursor()
        success = True
        try:
            for table, records in self.tables.items():
                table_vals = []
                table_order = self.table_columns[table]
                field_name_string = " ("
                value_string = " ("
                for column in table_order:
                    field_name_string += "`" + column + "`, "
                    if column in ('SubstantialCompletionYr', 'TaxYr', 'TaxYear', 'FormationYr', 'YearFormation'):
                        value_string += "CONCAT(%s, '-00-00'), "
                    elif column in ('ReturnTs', 'Timestamp'):
                        value_string += "LEFT(%s, 19), "
                    else:
                        value_string += "%s, "
                field_name_string = field_name_string[:-2] + ") "
                value_string = value_string[:-2] + ") "
                for row in records:
                    v = []
                    for column in table_order:
                        if column in row:
                            data_value = row[column]
                            if data_value == 'true':
                                data_value = 1
                            elif data_value == 'false':
                                data_value = 0
                            elif data_value == 'RESTRICTED':
                                data_value = None
                            v.append(data_value)
                        else:
                            v.append(None)
                    table_vals.append(tuple(v))

                table_query = "INSERT INTO irs." + table + " " + field_name_string + " VALUES " + value_string
                if len(table_vals) > 0 and not no_upload:
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        cursor.executemany(table_query, table_vals)
                        for w_elem in w:
                            success = False
                            index_list = ""
                            for val in table_vals:
                                index_list += str(val[0]) + ", "
                            logger.error(str(w_elem.message) + "\n Table: " + table + " Sub-batch:" + index_list)



        except Exception as e:
            success = False
            logger.error("Exception in batch commit")

        if success:
            logger.info("Executed batch successfully")
            connection.commit()
        else:
            logger.info("Rolling back batch")
            connection.rollback()

        self.tables.clear()
        return success

    def add_values(self, table_name, vals, connection):
        if table_name not in self.tables:
            self.add_table(table_name, vals, connection)
        self.tables[table_name].append(vals)


