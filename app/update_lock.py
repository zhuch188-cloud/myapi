"""进程内互斥：替代 MySQL GET_LOCK，避免并发重复插入 strategy_update_jobs。"""

import threading

_strategy_update_lock = threading.Lock()


def strategy_update_mutex() -> threading.Lock:
    return _strategy_update_lock
