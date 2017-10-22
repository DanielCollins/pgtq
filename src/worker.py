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
            task.execute()
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
    """
    while True:
        poll_loop(queue)
        queue.wait_for_a_task()
