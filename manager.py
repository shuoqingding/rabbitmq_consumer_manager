import pika
import requests
import threading
import time

from consumer import Consumer
from urllib import quote


class OpenConnectionTimeoutException(Exception):
    pass

class RabbitmqAdmin(object):
    admin_url = 'http://127.0.0.1:15672/api'
    uname = 'guest'
    pwd = 'guest'

    def __init__(self, ):
        self.session = requests.Session()
        self.session.auth = (self.uname, self.pwd)

    def list_queues(self, cols=None):
        cols = cols or []
        qs = '?columns=' + ','.join(cols)

        # We don't hanlde session disconnection as it will be
        # reconnected automaticlly
        res = self.session.get(self.admin_url + '/queues' + qs)

        # TODO: handle failure
        assert res.status_code == 200, "Request failed: %d" % res.status_code
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
        self.rabbitmq_admin = RabbitmqAdmin()

        # Create the connection pool
        for i in range(conn_num):
            conn = self.create_connection()
            self.conns.append(conn)

    def create_connection(self, ):
        conn = pika.SelectConnection(pika.URLParameters(self.amqp_url))
        t = threading.Thread(target=conn.ioloop.start)
        t.setDaemon(True)
        t.start()

        timeout = 10  # seconds
        warn_timeout = 2

        # Wait until the connection is open
        start = time.time()
        while not conn.is_open:
            t = time.time() - start
            if t > warn_timeout:
                print "It took too long to open the connection"
            elif t > timeout:
                raise OpenConnectionTimeoutException()
            time.sleep(0.1)

        return conn

    def get_queues(self, ):
        return self.rabbitmq_admin.list_queues(cols=['name', 'messages',
                                                     'messages_ready'])

    def create_consumer_for_queue(self, queue):
        # Choose the connection in the round-robin way
        self.conn_index = (self.conn_index + 1) % self.conn_num
        conn = self.conns[self.conn_index]

        # Restart the connection if it is closed
        if not conn.is_open:
            conn = self.create_connection()
            self.conns[self.conn_index] = conn

        assert conn.is_open, "Failed to create a connection"

        # Start a consumer thread
        c = Consumer(conn, self.amqp_url, queue)
        c.setDaemon(True)
        c.start()

        self.consumer_by_queue[queue] = c

    def run(self, ):
        while True:
            queue_infos = self.get_queues()
            for q in queue_infos:
                qname = q['name']
                consumer = self.consumer_by_queue.get(qname)

                # Clean consumers of empty queues
                if q['messages'] == 0 and consumer is not None:
                    # TODO: handle pika.exceptions.ConnectionClosed
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

