import xmlrpc.client
import tkinter
import dp_util.app_util as app_util

if __name__ == "__main__":
    s = xmlrpc.client.ServerProxy('http://localhost:9999')
    #todo: pass proxy to interface?
    root = tkinter.Tk()
    app = app_util.Application(master=root)
    logger.info("Initialization complete")
    app.mainloop()
    logger.info("Closing connections")
    logging.captureWarnings(False)
    connection.close()
    print(s.test_function(2,3))