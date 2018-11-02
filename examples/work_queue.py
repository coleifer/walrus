"""
Simple multi-process task queue using Redis Streams.

http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/
"""
from collections import namedtuple
from functools import wraps
import datetime
import multiprocessing
import pickle
import time

# At the time of writing, the standard redis-py client does not implement
# stream/consumer-group commands. We'll use "walrus", which extends the client
# from redis-py to provide stream support and high-level, Pythonic containers.
# More info: https://github.com/coleifer/walrus
from walrus import Walrus


# Lightweight wrapper for storing exceptions that occurred executing a task.
TaskError = namedtuple('TaskError', ('error',))


class TaskQueue(object):
    def __init__(self, client, stream_key='tasks'):
        self.client = client  # our Redis client.
        self.stream_key = stream_key

        # We'll also create a consumer group (whose name is derived from the
        # stream key). Consumer groups are needed to provide message delivery
        # tracking and to ensure that our messages are distributed among the
        # worker processes.
        self.name = stream_key + '-cg'
        self.consumer_group = self.client.consumer_group(self.name, stream_key)
        self.result_key = stream_key + '.results'  # Store results in a Hash.

        # Obtain a reference to the stream within the context of the
        # consumer group.
        self.stream = getattr(self.consumer_group, stream_key)
        self.signal = multiprocessing.Event()  # Used to signal shutdown.
        self.signal.set()  # Indicate the server is not running.

        # Create the stream and consumer group (if they do not exist).
        self.consumer_group.create()
        self._running = False
        self._tasks = {}  # Lookup table for mapping function name -> impl.

    def task(self, fn):
        self._tasks[fn.__name__] = fn  # Store function in lookup table.

        @wraps(fn)
        def inner(*args, **kwargs):
            # When the decorated function is called, a message is added to the
            # stream and a wrapper class is returned, which provides access to
            # the task result.
            message = self.serialize_message(fn, args, kwargs)

            # Our message format is very simple -- just a "task" key and a blob
            # of pickled data. You could extend this to provide additional
            # data, such as the source of the event, etc, etc.
            task_id = self.stream.add({'task': message})
            return TaskResultWrapper(self, task_id)
        return inner

    def deserialize_message(self, message):
        task_name, args, kwargs = pickle.loads(message)
        if task_name not in self._tasks:
            raise Exception('task "%s" not registered with queue.')
        return self._tasks[task_name], args, kwargs

    def serialize_message(self, task, args=None, kwargs=None):
        return pickle.dumps((task.__name__, args, kwargs))

    def store_result(self, task_id, result):
        # API for storing the return value from a task. This is called by the
        # workers after the execution of a task.
        if result is not None:
            self.client.hset(self.result_key, task_id, pickle.dumps(result))

    def get_result(self, task_id):
        # Obtain the return value of a finished task. This API is used by the
        # TaskResultWrapper class. We'll use a pipeline to ensure that reading
        # and popping the result is an atomic operation.
        pipe = self.client.pipeline()
        pipe.hexists(self.result_key, task_id)
        pipe.hget(self.result_key, task_id)
        pipe.hdel(self.result_key, task_id)
        exists, val, n = pipe.execute()
        return pickle.loads(val) if exists else None

    def run(self, nworkers=1):
        if not self.signal.is_set():
            raise Exception('workers are already running')

        # Start a pool of worker processes.
        self._pool = []
        self.signal.clear()
        for i in range(nworkers):
            worker = TaskWorker(self)
            worker_t = multiprocessing.Process(target=worker.run)
            worker_t.start()
            self._pool.append(worker_t)

    def shutdown(self):
        if self.signal.is_set():
            raise Exception('workers are not running')

        # Send the "shutdown" signal and wait for the worker processes
        # to exit.
        self.signal.set()
        for worker_t in self._pool:
            worker_t.join()


class TaskWorker(object):
    _worker_idx = 0

    def __init__(self, queue):
        self.queue = queue
        self.consumer_group = queue.consumer_group

        # Assign each worker processes a unique name.
        TaskWorker._worker_idx += 1
        worker_name = 'worker-%s' % TaskWorker._worker_idx
        self.worker_name = worker_name

    def run(self):
        while not self.queue.signal.is_set():
            # Read up to one message, blocking for up to 1sec, and identifying
            # ourselves using our "worker name".
            resp = self.consumer_group.read(1, 1000, self.worker_name)
            if resp is not None:
                # Resp is structured as:
                # {stream_key: [(message id, data), ...]}
                for stream_key, message_list in resp:
                    task_id, data = message_list[0]
                    self.execute(task_id.decode('utf-8'), data[b'task'])

    def execute(self, task_id, message):
        # Deserialize the task message, which consists of the task name, args
        # and kwargs. The task function is then looked-up by name and called
        # using the given arguments.
        task, args, kwargs = self.queue.deserialize_message(message)
        try:
            ret = task(*(args or ()), **(kwargs or {}))
        except Exception as exc:
            # On failure, we'll store a special "TaskError" as the result. This
            # will signal to the user that the task failed with an exception.
            self.queue.store_result(task_id, TaskError(str(exc)))
        else:
            # Store the result and acknowledge (ACK) the message.
            self.queue.store_result(task_id, ret)
            self.queue.stream.ack(task_id)


class TaskResultWrapper(object):
    def __init__(self, queue, task_id):
        self.queue = queue
        self.task_id = task_id
        self._result = None

    def __call__(self, block=True, timeout=None):
        if self._result is None:
            # Get the result from the result-store, optionally blocking until
            # the result becomes available.
            if not block:
                result = self.queue.get_result(self.task_id)
            else:
                start = time.time()
                while timeout is None or (start + timeout) > time.time():
                    result = self.queue.get_result(self.task_id)
                    if result is None:
                        time.sleep(0.1)
                    else:
                        break

            if result is not None:
                self._result = result

        if self._result is not None and isinstance(self._result, TaskError):
            raise Exception('task failed: %s' % self._result.error)

        return self._result


if __name__ == '__main__':
    db = Walrus()  # roughly equivalent to db = Redis().
    queue = TaskQueue(db)

    @queue.task
    def sleep(n):
        print('going to sleep for %s seconds' % n)
        time.sleep(n)
        print('woke up after %s seconds' % n)

    @queue.task
    def fib(n):
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return b

    # Start the queue with four worker processes.
    queue.run(4)

    # Send four "sleep" tasks.
    sleep(2)
    sleep(3)
    sleep(4)
    sleep(5)

    # Send four tasks to compute large fibonacci numbers. We will then print the
    # last 6 digits of each computed number (showing how result storage works):
    v100k = fib(100000)
    v200k = fib(200000)
    v300k = fib(300000)
    v400k = fib(400000)

    # Calling the result wrapper will block until its value becomes available:
    print('100kth fib number starts ends with: %s' % str(v100k())[-6:])
    print('200kth fib number starts ends with: %s' % str(v200k())[-6:])
    print('300kth fib number starts ends with: %s' % str(v300k())[-6:])
    print('400kth fib number starts ends with: %s' % str(v400k())[-6:])

    # We can shutdown and restart the consumer.
    queue.shutdown()
    print('all workers have stopped.')

    queue.run(4)
    print('workers are running again.')

    # Enqueue another "sleep" task.
    sleep(2)

    # Calling shutdown now will block until the above sleep task
    # has finished, after which all workers will stop.
    queue.shutdown()
    print('done!')
