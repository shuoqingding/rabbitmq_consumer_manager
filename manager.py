import pika
import requests
import threading
import time

from consumer import Consumer
from urllib import quote

class RabbitmqAdmin(object):
    admin_url = 'http://127.0.0.1:15672/api'
    uname = 'guest'
    pwd = 'guest'

    def list_queues(self, cols=None):
        cols = cols or []
        qs = '?columns=' + ','.join(cols)

        res = requests.get(self.admin_url + '/queues' + qs,
                           auth=(self.uname, self.pwd))
        assert res.status_code == 200, "Request failed: %d" % res.status_code
        # TODO: handle failure
        return res.json()


class ConsumerManager(object):

    REFRESH_INTERVAL = 5  # seconds

    def __init__(self, vhost='/', conn_num=1):
        self.consumer_by_queue = {}
        self.vhost = quote(vhost, safe='')
        self.amqp_url = 'amqp://guest:guest@localhost:5672/' + self.vhost

        self.conn_num = conn_num
        self.conns = []
        self.conn_index = 0
        # Create the connection pool
        for i in range(conn_num):
            conn = pika.SelectConnection(pika.URLParameters(self.amqp_url))
            threading.Thread(target=conn.ioloop.start).start()
            self.conns.append(conn)

    def get_queues(self, ):
        # TODO: use long-live connection
        admin = RabbitmqAdmin()
        return admin.list_queues(cols=['name', 'messages', 'messages_ready'])

    def create_consumer_for_queue(self, queue):
        # TODO: restart connection if closed
        assert self.conns[self.conn_index].is_open
        c = Consumer(self.conns[self.conn_index], self.amqp_url, queue)
        c.setDaemon(True)
        c.start()

        # Choose the connection in the round-robin way
        self.conn_index = (self.conn_index + 1) % self.conn_num

        self.consumer_by_queue[queue] = c

    def run(self, ):
        while True:
            queue_infos = self.get_queues()
            for q in queue_infos:
                qname = q['name']
                consumer = self.consumer_by_queue.get(qname)

                # Clean consumers of empty queues
                if q['messages'] == 0 and consumer is not None:
                    self.consumer_by_queue[qname].stop()
                    del self.consumer_by_queue[qname]

                # Create consumers for queues with ready message
                if q['messages_ready'] > 0:
                    if consumer is None or not consumer.isAlive():
                        print "Creating thread for queue %s" % qname
                        self.create_consumer_for_queue(qname)

            time.sleep(self.REFRESH_INTERVAL)


if __name__ == "__main__":
    cm = ConsumerManager()
    cm.run()

