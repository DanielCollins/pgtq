"""A task queue"""
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
    def handler(self, name=None):
        """Return a decorator for creating new handlers."""
        def decorator(procedure):
            """Create a new handler from the decorated function."""
            nonlocal name
            if not name:
                name = procedure.__name__
            new_handler = handler.Handler(self, procedure, name)
            if new_handler.name in self.handlers:
                err = "Conflict: handler for task '{}' already exists."
                raise RuntimeError(err.format(new_handler.name))
            self.handlers[new_handler.name] = new_handler
            return new_handler
        return decorator

    def push(self, handler_name, args, kwargs):
        """Insert a task into the end of the queue."""
        sql_template = """
           INSERT INTO pgtq_{0}_runnable (task) VALUES (%s);
        """
        sql = sql_template.format(self.name)
        serialised_task = psycopg2.extras.Json({'name': handler_name,
                                                'args': args,
                                                'kwargs': kwargs})
        channel = "pgtq_{0}_runnable_channel".format(self.name)
        notification = "NOTIFY {};".format(channel)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [serialised_task])
                cursor.execute(notification)

    def pop(self):
        """Remove a task from the start of the queue, returning it."""
        sql = "EXECUTE pgtq_{0}_lock_task;".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                json_repr = cursor.fetchone()
                if json_repr:
                    return task.Task(self, json_repr)

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

    def mark_completed(self, task_key):
        """Move the given task from the running set to the completed set."""
        sql = "EXECUTE pgtq_{0}_mark_completed (%s);".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [task_key])

    def mark_interupted(self, task_key):
        """Move the given task from the running set to the interupted set."""
        sql = "EXECUTE pgtq_{0}_mark_interupted (%s);".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [task_key])
