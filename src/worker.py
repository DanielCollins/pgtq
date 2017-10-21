import datetime

MAX_POLLING_IDLE_TIME = datetime.timedelta(seconds=30)
MIN_NOTIFY_IDLE_TIME = datetime.timedelta(seconds=60)


def handle_task(_task):
    raise NotImplementedError


def poll_loop(queue):
    time_of_last_task = datetime.datetime.utcnow()
    while True:
        task = queue.get_a_task()
        now = datetime.datetime.utcnow()
        if task:
            handle_task(task)
            time_of_last_task = now
        else:
            time_since_last_task = now - time_of_last_task
            if time_since_last_task > MAX_POLLING_IDLE_TIME:
                return  # switch to notify loop


def notify_loop(queue):
    time_of_last_task = datetime.datetime.utcnow()
    while True:
        task = queue.wait_on_a_task()
        handle_task(task)
        now = datetime.datetime.utcnow()
        time_since_last_task = now - time_of_last_task
        if time_since_last_task < MIN_NOTIFY_IDLE_TIME:
            return  # switch to poll loop
        time_of_last_task = datetime.datetime.utcnow()


def main_loop(queue):
    while True:
        poll_loop(queue)
        notify_loop(queue)
