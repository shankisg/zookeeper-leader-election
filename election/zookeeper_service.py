from kazoo.client import KazooClient


class ZookeeperService(object):
    def __init__(self, host, port):
        self.zk = KazooClient(hosts='{}:{}'.format(host, port))
        self.zk.start()

    def get(self, path):
        return self.zk.get(path)

    def get_children(self, path):
        return self.zk.get_children(path)

    def exists(self, path):
        return self.zk.exists(path)
    
    def ensure_path(self, path):
        return self.zk.ensure_path(path)

    def create(self, path, ephemeral=False, sequence=False):
        return self.zk.create(path, ephemeral=True, sequence=True)
    
    def watch_node(self, path, watch=None):
        return self.zk.exists(path, watch=watch)
