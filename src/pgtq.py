"""A task queue."""
import select
import psycopg2.extras
import handler
import schema
import task


class PgTq(object):
    """Represents a single task queue."""

    def __init__(self, name, connection_string):
        """Create a task queue with the given name in the given DB."""
        self.name = name
        self.connection_string = connection_string
        self.conn = psycopg2.connect(connection_string)
        self.create_tables()
        self.handlers = {}

    def create_tables(self):
        """Ensure that the structures needed to store tasks exist."""
        sql = schema.SQL_TEMPLATE.format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)

    # pylint: disable=unused-argument
    def handler(self, name=None, max_retries=None):
        """Return a decorator for creating new handlers."""
        if max_retries is not None and max_retries < 0:
            raise ValueError("max_retries must be positive")

        def decorator(procedure):
            """Create a new handler from the decorated function."""
            nonlocal name
            if not name:
                name = procedure.__name__
            new_handler = handler.Handler(self, procedure, name, max_retries)
            if new_handler.name in self.handlers:
                err = "Conflict: handler for task '{}' already exists."
                raise RuntimeError(err.format(new_handler.name))
            self.handlers[new_handler.name] = new_handler
            return new_handler
        return decorator

    def push(self, handler_name, max_retries, args, kwargs):
        """Insert a task into the end of the queue."""
        sql_template = "SELECT pgtq_{0}_push(%s, %s);"
        sql = sql_template.format(self.name)
        serialised_task = psycopg2.extras.Json({'name': handler_name,
                                                'args': args,
                                                'kwargs': kwargs})
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [serialised_task, max_retries])

    def pop(self):
        """Remove a task from the start of the queue, returning it."""
        sql = "EXECUTE pgtq_{0}_lock_task;".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                json_repr = cursor.fetchone()
                if json_repr:
                    return task.Task(self, json_repr)

    def run_scheduled(self):
        """Move scheduled tasks into task queue.

        Any and all schedule items, including failed task retries, that
        are at or past their scheduled time, are pushed onto the end of
        the task queue to be picked up by free workers.

        Return the time of the next scheduled task.
        """
        sql = "SELECT pgtq_{0}_run_scheduled();".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()[0]

    def wait_for_a_task(self):
        """Block the thread until the DB notifies a task exists.

        In the presense of multiple worker processes, there is no
        garentee that a task will exist when this method returns.
        """
        connection = psycopg2.connect(self.connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        channel = "pgtq_{0}_runnable_channel".format(self.name)
        cursor.execute("LISTEN {};".format(channel))
        while True:
            select.select([connection], [], [])
            connection.poll()
            if connection.notifies:
                cursor.execute("UNLISTEN {};".format(channel))
                cursor.close()
                connection.close()
                return

    def wait_for_a_schedule(self, timeout):
        """Wait for a new shceduled item.

        Block the thread until the DB notifies that a new task has been
        scheduled, up to timeout seconds.
        """
        connection = psycopg2.connect(self.connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        channel = "pgtq_{0}_scheduled_channel".format(self.name)
        cursor.execute("LISTEN {};".format(channel))
        while True:
            select.select([connection], [], [], timeout)
            connection.poll()
            if connection.notifies:
                cursor.execute("UNLISTEN {};".format(channel))
                cursor.close()
                connection.close()

    def mark_completed(self, task_key):
        """Move the given task from the running set to the completed set."""
        sql = "EXECUTE pgtq_{0}_mark_completed (%s);".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [task_key])

    def mark_interupted(self, task_key):
        """Move the given task from the running set to the interupted set."""
        sql = "SELECT pgtq_{0}_interupt(%s);".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [task_key])
