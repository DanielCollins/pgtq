import psycopg2
import handler
import schema


class PgTq(object):

    def __init__(self, name, connection_string):
        self.name = name
        self.conn = psycopg2.connect(connection_string)
        self.create_tables()

    def create_tables(self):
        sql = schema.sql_template.format(self.name)
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
                cursor.execute(sql, serialised_task)
