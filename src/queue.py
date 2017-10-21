import psycopg2
import task
import schema


class PgTq(object):

    def __init__(self, name, connection_string):
        self.name = name
        self.conn = psycopg2.connect(connection_string)

    def create_tables(self):
        sql = schema.sql_template.format(self.name)
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)

    def task(self, procedure):
        return task.Task(self, procedure)

    def push(self, task_name, args, kwargs):
        sql_template = """
           INSERT INTO pgtq_{1}_runnable (task) VALUES (%s);
        """
        sql = sql_template.format(self.name)
        serialised_task = psycopg2.extras.Json({'name': task_name,
                                                'args': args,
                                                'kwargs': kwargs})
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, serialised_task)
