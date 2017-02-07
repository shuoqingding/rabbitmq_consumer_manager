import requests
import time
import threading



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
    def __init__(self, ):
        self.consumer_by_queue = {}

    def get_non_empty_queues(self, ):
        admin = RabbitmqAdmin()
        queues = admin.list_queues(cols=['name', 'messages_ready'])
        non_empty_queues = [q['name'] for q in queues if q['messages_ready'] > 0]
        return non_empty_queues

    def create_consumer_for_queue(self, queue):
        c = RabbitmqConsumer(callback_on_exit=remove_consumer)
        c.setDaemon(True)
        c.start()

        self.consumer_by_queue[queue] = c

    def run(self, ):
        while True:
            non_empty_queues = self.get_non_empty_queues()

            for q in non_empty_queues:
                consumer = self.consumer_by_queue.get(q)
                if consumer is None or not consumer.isAlive():
                    print "Creating thread for queue %s" % q
                    self.create_consumer_for_queue(q)
                else:
                    print "There is already a consumer for queue %s" % q

            time.sleep(5)


class RabbitmqConsumer(threading.Thread):

    # TODO: Add heartbeat?
    def run(self, ):
        consumer_name = self.getName()

        for i in range(5):
            print "Consumer %s Running..." % consumer_name
            time.sleep(5)

        print "Consumer %s Exited" % consumer_name


if __name__ == "__main__":
    cm = ConsumerManager()
    cm.run()

