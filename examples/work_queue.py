import threading
import time

from walrus import Walrus


db = Walrus()

def create_consumer_group():
    consumer = db.consumer_group('tasks-cg', ['tasks'])
    if not db.exists('tasks'):
        db.xadd('tasks', {'dummy': ''}, id=b'0-1')
        consumer.create()
        consumer.set_id('$')
    return consumer

def worker(tid, consumer_group, stop_signal):
    # Each worker thread runs as its own consumer within the group.
    consumer = consumer_group.consumer('worker-%s' % tid)

    while not stop_signal.is_set():
        messages = consumer.tasks.read(count=1, timeout=1000)
        if messages is not None:
            message_id, data = messages[0]
            print('worker %s processing: %s' % (tid, data))
            consumer.tasks.ack(message_id)

def main():
    consumer_group = create_consumer_group()
    stream = consumer_group.tasks

    stop_signal = threading.Event()
    workers = []
    for i in range(4):
        worker_t = threading.Thread(target=worker,
                                    args=(i + 1, consumer_group, stop_signal))
        worker_t.daemon = True
        workers.append(worker_t)

    print('Seeding stream with 10 events')
    for i in range(10):
        stream.add({'data': 'event %s' % i})

    print('Starting worker pool')
    for worker_t in workers:
        worker_t.start()

    print('Adding 20 more messages, 4 per second')
    for i in range(10, 30):
        print('Adding event %s' % i)
        stream.add({'data': 'event %s' % i})
        time.sleep(0.25)

    stop_signal.set()
    [t.join() for t in workers]


if __name__ == '__main__':
    main()
