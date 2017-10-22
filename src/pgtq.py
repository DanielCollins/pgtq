import select
import psycopg2.extras
import handler
import schema


class PgTq(object):

    def __init__(self, name, connection_string):
        self.name = name
        self.connection_string = connection_string
        self.conn = psycopg2.connect(connection_string)
        self.create_tables()

    def create_tables(self):
        sql = schema.SQL_TEMPLATE.format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)

    def handler(self):
        def decorator(procedure):
            return handler.Handler(self, procedure)
        return decorator

    def push(self, handler_name, args, kwargs):
        sql_template = """
           INSERT INTO pgtq_{0}_runnable (task) VALUES (%s);
        """
        sql = sql_template.format(self.name)
        serialised_task = psycopg2.extras.Json({'name': handler_name,
                                                'args': args,
                                                'kwargs': kwargs})
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [serialised_task])

    def get_a_task(self):
        sql = "pgtq_{0}_lock_task".format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()

    def wait_for_a_task(self):
        connection = psycopg2.connect(self.connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("LISTEN data;")
        while True:
            select.select([connection], [], [])
            connection.poll()
            if connection.notifies:
                cursor.execute("UNLISTEN data;")
                cursor.close()
                connection.close()
                return
