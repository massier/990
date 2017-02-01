import dp_util.db_util as db_util

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

if __name__ == "__main__":
    db_util.init_logging()
    server = SimpleXMLRPCServer(('localhost', 9999), requestHandler=RequestHandler)
    server.register_introspection_functions()
    def test_function(x,y):
        return x + y
    server.register_function(test_function)


    server.serve_forever()
