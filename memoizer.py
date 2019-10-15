# from http://codereview.stackexchange.com/questions/91656/thread-safe-memoizer

from threading import Thread, Lock
from functools import wraps


class CacheEntry: pass


def memoized(f):
    """Decorator that caches a function's result each time it is called.
    If called later with the same arguments, the cached value is
    returned, and not re-evaluated. Access to the cache is
    thread-safe.

    """
    cache = {}                  # Map from key to CacheEntry
    cache_lock = Lock()         # Guards cache

    @wraps(f)
    def memoizer(*args, **kwargs):
        key = args, tuple(kwargs.items())
        result_lock = None

        with cache_lock:
            try:
                entry = cache[key]
            except KeyError:
                entry = cache[key] = CacheEntry()
                result_lock = entry.lock = Lock() # Guards entry.result
                result_lock.acquire()

        if result_lock:
            # Compute the new entry without holding cache_lock, to avoid
            # deadlock if the call to f is recursive (in which case
            # memoizer is re-entered).
            result = entry.result = f(*args, **kwargs)
            result_lock.release()
            return result
        else:
            # Wait on entry.lock without holding cache_lock, to avoid
            # blocking other threads that need to use the cache.
            with entry.lock:
                return entry.result

    return memoizer


if __name__ == "__main__":
    from threading import current_thread
    import time

    @memoized
    def f(x):
        time.sleep(2)
        return x * 10

    lock_write = Lock()
    def work(x):
        result = f(x)
        with lock_write:
            print(current_thread(), x, result)

    work(1)
    work(1)
    work(2)


    threads = []

    start = time.time()
    for i in (0, 1, 1, 2, 0, 0, 0, 1, 3):
        t = Thread(target=work, args=(i,))
        threads.append(t)
        t.start()

    time.sleep(1)

    for i in (0, 1, 1, 2, 0, 0, 0, 1, 3):
        t = Thread(target=work, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(time.time() - start)
