import sys
import logging
from zookeeper_service import ZookeeperService


log = logging.basicConfig(level=logging.INFO)

class ProcessNode(object):
    ROOT_PATH = "/election"
    CHILD_PREFIX = "/pre_"
    CHILD_PATH = "{}{}".format(ROOT_PATH, CHILD_PREFIX)

    def __init__(self, host, port):
        self.zk = ZookeeperService(host, port)
        self.process_path = ""

    def attempt_leader_election(self):
        all_children = self.zk.get_children(self.ROOT_PATH)
        all_children.sort()
        process_path = self.process_path.split("/")[-1]
        process_index = all_children.index(process_path)

        if process_index == 0:
            logging.info("I am the leader")
        else:
            prev_process = all_children[process_index - 1]
            prev_process = "{}/{}".format(self.ROOT_PATH, prev_process)
            logging.info("Following previous znode {}".format(prev_process))
            self.zk.watch_node(prev_process, watch=self.process_watch)            

    def process_watch(self, event):
        if event.type == 'DELETED':
            logging.info("Attempting leader election...")            
            self.attempt_leader_election()       
 
    def run(self):
        root_node = self.zk.ensure_path(self.ROOT_PATH)
        self.process_path = self.zk.create(self.CHILD_PATH, ephemeral=True, sequence=True)
        self.attempt_leader_election()        


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Provide host and port as first and second argument.")
        sys.exit(1)
    host, port = sys.argv[1], sys.argv[2]

    flag = False
    while True:
        if not flag:
            node = ProcessNode("127.0.0.1", 2181)
            node.run()
            logging.info("Starting process: {}".format(node.process_path))
            flag = True

