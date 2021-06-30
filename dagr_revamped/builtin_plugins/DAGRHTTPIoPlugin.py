from .classes.DAGRHTTPIo import DAGRHTTPIo

def setup(manager):
    manager.register_io('DAGRHTTPIo', DAGRHTTPIo)
    return True