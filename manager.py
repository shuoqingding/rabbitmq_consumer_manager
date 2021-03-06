import logging
import pika
import requests
import threading
import time
from urllib import quote

import conf
from consumer import Consumer


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=conf.LOG_FORMAT)


class OpenConnectionTimeoutException(Exception):
    """
    Raised if we take too long to open a rabbitmq connection.
    """
    pass


class AdminAPIError(Exception):
    """
    Raised if unexpected behavior happens in a rabbitmq admin request.
    """
    pass


class PermissionError(Exception):
    pass


class RabbitmqAdmin(object):
    """
    Client for using RabbitMQ Management API.
    """
    admin_url = 'http://%s:%s/api' % (conf.RABBITMQ_HOST,
                                      conf.RABBITMQ_ADMIN_PORT)
    uname = conf.RABBITMQ_USER_NAME
    pwd = conf.RABBITMQ_PASSWORD

    def __init__(self, ):
        self.session = requests.Session()
        self.session.auth = (self.uname, self.pwd)

    def _call(self, uri, querystring=''):
        url = '%s/%s?%s' % (self.admin_url, uri, querystring)
        res = self.session.get(url)
        if res.status_code == requests.codes.unauthorized:
            raise PermissionError()

        return res

    def list_queues(self, cols=None):
        LOGGER.debug("Listing queues...")
        cols = cols or []
        qs = 'columns=' + ','.join(cols)

        # We don't hanlde session disconnection as it will be
        # reconnected automaticlly
        res = self._call('queues', qs)
        if res.status_code != requests.codes.ok:
            LOGGER.error("Request failed. status_code: %d, respond: %s", res.status_code, res.text)
            # TODO: handle failure
            raise AdminAPIError(res)

        LOGGER.debug("Result: %s" % res.text)
        return res.json()


class ConsumerManager(object):

    REFRESH_INTERVAL = 3  # seconds

    def __init__(self, vhost='/', conn_num=1):
        # TODO: make consumer number per queue configurable
        self.consumer_by_queue = {}
        self.vhost = quote(vhost, safe='')
        self.amqp_url = 'amqp://%s:%s@%s:%s/%s' % (
            conf.RABBITMQ_USER_NAME,
            conf.RABBITMQ_PASSWORD,
            conf.RABBITMQ_HOST,
            conf.RABBITMQ_AMQP_PORT,
            self.vhost,
        )

        self.conn_num = conn_num
        self.conns = []
        self.conn_index = 0
        self.rabbitmq_admin = RabbitmqAdmin()

        # Create the connection pool
        LOGGER.info("Creating connection pool of size %d" % self.conn_num)
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
                LOGGER.warn("It took too long to open connection")
            elif t > timeout:
                LOGGER.error("Open connection timeout")
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
            LOGGER.warn("Connection closd, reconnectting...")
            conn = self.create_connection()
            self.conns[self.conn_index] = conn

        # Sanity check
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
                    LOGGER.info("Closing idle consumer for queue %s", qname)
                    self.consumer_by_queue[qname].stop()
                    del self.consumer_by_queue[qname]

                # Create consumers for queues with ready message
                if q['messages_ready'] > 0:
                    if consumer is None or not consumer.is_running():
                        LOGGER.info("Creating thread for queue %s", qname)
                        self.create_consumer_for_queue(qname)

            time.sleep(self.REFRESH_INTERVAL)


if __name__ == "__main__":
    cm = ConsumerManager()
    cm.run()

