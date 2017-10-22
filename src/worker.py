"""Worker process continually fetches and processes tasks."""

import datetime

MAX_POLLING_IDLE_TIME = datetime.timedelta(seconds=30)


def poll_loop(queue):
    """Continually fetch and handle tasks from the queue."""
    time_of_last_task = datetime.datetime.utcnow()
    while True:
        task = queue.pop()
        now = datetime.datetime.utcnow()
        if task:
            try:
                task.execute()
            # pylint: disable=broad-except
            except Exception:
                # this should not happen, but there is no reason to
                # crash the entire worker process if it does
                pass
            time_of_last_task = now
        else:
            time_since_last_task = now - time_of_last_task
            if time_since_last_task > MAX_POLLING_IDLE_TIME:
                # no tasks recently, go to sleep
                return


def main_loop(queue):
    """Continually fetch and handle tasks from the given queue.

       Starts by polling for tasks in a tight loop. If no tasks arrive
       for MAX_POLLING_IDLE_TIME, go to sleep until notified by the
       database that another task is ready.

       This function does not return.
    """
    while True:
        try:
            poll_loop(queue)
        # pylint: disable=broad-except
        except Exception:
            # this should not happen, but there is no reason to crash
            # the entire worker process if it does
            pass
        try:
            queue.wait_for_a_task()
        # pylint: disable=broad-except
        except Exception:
            # this should not happen, but there is no reason to crash
            # the entire worker process if it does
            pass
