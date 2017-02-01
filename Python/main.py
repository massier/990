import logging
import tkinter

import dp_util.db_util as db_util
import dp_util.log_util as log_util
import dp_util.app_util as app_util
import dp_util.schema_util as schema_util

if __name__ == "__main__":
    db_util.init_logging()
    logger = logging.getLogger('main_logger')
    connection_new = db_util.make_test_connection()

    # create anonymous client connection
    s3_new = app_util.establish_s3_connection()
    root = tkinter.Tk()
    app = app_util.Application(master=root, s3=s3_new, connection=connection_new)
    logger.info("Initialization complete")
    app.mainloop()
    logger.info("Closing connections")
    logging.captureWarnings(False)
    connection_new.close()