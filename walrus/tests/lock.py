import threading
import time

from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestLock(WalrusTestCase):
    def test_lock_edge_cases(self):
        # A waiter retries once an abandoned, short-lived lock expires.
        db.lock('short', ttl=500).acquire()
        self.assertTrue(db.lock('short', ttl=500).acquire())

        # Threads sharing one instance hold their own tokens.
        lock = db.lock('shared', ttl=100)
        lock.acquire()
        time.sleep(0.2)
        t = threading.Thread(target=lock.acquire)
        t.start()
        t.join()
        self.assertFalse(lock.release())

        # A failed release does not hide the body's exception.
        with self.assertRaises(ValueError):
            with db.lock('m', ttl=100):
                time.sleep(0.2)
                raise ValueError

    def test_lock(self):
        lock_a = db.lock('lock-a')
        lock_b = db.lock('lock-b')
        self.assertTrue(lock_a.acquire())
        self.assertTrue(lock_b.acquire())

        lock_a2 = db.lock('lock-a')
        self.assertFalse(lock_a2.acquire(block=False))
        self.assertFalse(lock_a2.release())

        self.assertFalse(lock_a.acquire(block=False))
        self.assertFalse(lock_b.acquire(block=False))

        t_waiting = threading.Event()
        t_acquired = threading.Event()
        t_acknowledged = threading.Event()

        def wait_for_lock():
            lock_a = db.lock('lock-a')
            t_waiting.set()
            lock_a.acquire()
            t_acquired.set()
            t_acknowledged.wait()
            lock_a.release()

        waiter_t = threading.Thread(target=wait_for_lock)
        waiter_t.start()
        t_waiting.wait()  # Wait until the thread is up and running.

        lock_a.release()
        t_acquired.wait()
        self.assertFalse(lock_a.acquire(block=False))
        t_acknowledged.set()
        waiter_t.join()
        self.assertTrue(lock_a.acquire(block=False))
        lock_a.release()

    def test_lock_ctx_mgr(self):
        lock_a = db.lock('lock-a')
        lock_a2 = db.lock('lock-a')
        with lock_a:
            self.assertFalse(lock_a2.acquire(block=False))
        self.assertTrue(lock_a2.acquire(block=False))

    def test_lock_decorator(self):
        lock = db.lock('lock-a')

        @lock
        def locked():
            lock2 = db.lock('lock-a')
            self.assertFalse(lock2.acquire(block=False))

        locked()

        @lock
        def raise_exception():
            raise ValueError()

        self.assertRaises(ValueError, raise_exception)

        # In the event of an exception, the lock will still be released.
        self.assertTrue(lock.acquire(block=False))

    def test_semaphore(self):
        sem = db.semaphore('sem', 2)
        self.assertTrue(sem.acquire())
        self.assertTrue(sem.acquire())  # One thread may hold several slots.
        self.assertFalse(db.semaphore('sem', 2).acquire(block=False))

        # A blocked waiter takes the slot that is released.
        t = threading.Thread(target=sem.acquire)
        t.start()
        self.assertTrue(sem.release())
        t.join()
        self.assertFalse(sem.acquire(block=False))

        # The slot of an expired holder frees itself.
        db.semaphore('short', 1, ttl=100).acquire()
        time.sleep(0.2)
        self.assertTrue(db.semaphore('short', 1).acquire(block=False))

    def test_renew(self):
        lock = db.lock('renew', ttl=100)
        self.assertFalse(lock.held())
        lock.acquire()
        self.assertTrue(lock.renew(5000))
        time.sleep(0.2)
        self.assertTrue(lock.held())
        lock.release()
        self.assertFalse(lock.renew())

        # A background thread keeps a short TTL alive until release.
        for obj in (db.lock('auto', ttl=150, auto_renew=True),
                    db.semaphore('auto', 1, ttl=150, auto_renew=True)):
            with obj:
                time.sleep(0.4)
                self.assertTrue(obj.held())
            self.assertFalse(obj.held())

        self.assertRaises(ValueError, db.lock, 'x', auto_renew=True)

    def test_event(self):
        event = db.event('e')
        self.assertFalse(event.wait(0.1))

        # One set() wakes every waiter.
        results = []
        threads = [threading.Thread(
            target=lambda: results.append(event.wait(5))) for _ in range(3)]
        for t in threads: t.start()
        event.set()
        for t in threads: t.join()
        self.assertEqual(results, [True, True, True])

        event.clear()
        self.assertFalse(event.is_set())

    def test_barrier(self):
        barrier = db.barrier('b', 3)
        results = []

        def run_round():
            threads = [threading.Thread(
                target=lambda: results.append(barrier.wait(5)))
                for _ in range(3)]
            for t in threads: t.start()
            for t in threads: t.join()

        run_round()
        run_round()  # The barrier resets after each release.
        self.assertEqual(results, [True] * 6)

        # A caller that times out withdraws.
        self.assertFalse(barrier.wait(0.1))
        self.assertEqual(db.hget(barrier.key, 'count'), b'0')

    def test_lock_cleanup(self):
        self.assertEqual(len(db), 0)
        lock = db.lock('lock-a')
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())
        self.assertEqual(len(db), 1)  # We have the lock event key.
        self.assertEqual(db.lpop(lock.event), b'1')
