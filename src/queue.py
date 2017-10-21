import psycopg2
import task

sql_template = """
CREATE TABLE IF NOT EXISTS pgtq_{1}_scheduled (
  key INTEGER PRIMARY KEY,
  not_before TIMESTAMP WITHOUT TIME ZONE,
  task JSON,
  retried INTEGER
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS
  ix_pgtq_{1}_scheduled_not_before ON pgtq_{1}_scheduled (not_before);

CREATE TABLE IF NOT EXISTS pgtq_{1}_runnable (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pgtq_{1}_running (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE IF NOT EXISTS pgtq_{1}_complete (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

PREPARE pgtq_{1}_lock_task AS
  WITH task
  AS (DELETE FROM pgtq_{1}_runnable a
      WHERE a.ctid = (SELECT ctid
                      FROM pgtq_{1}_runnable
                      ORDER BY key
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
      RETURNING *)
    INSERT INTO pgtq_{1}_running SELECT * FROM task RETURNING *;

PREPARE pgtq_{1}_mark_completed (INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_{1}_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_{1}_complete SELECT * FROM task RETURNING *;

PREPARE pgtq_{1}_mark_interupted(INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_{1}_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_{1}_scheduled
    SELECT key,
           (now() at time zone 'utc') +
            (INTERVAL '1 second' * random() * retried),
           task,
           (retried + 1)
    FROM task RETURNING *;

PREPARE pgtq_{1}_run_scheduled AS
  WITH ready AS
    (DELETE FROM pqtq_{1}_scheduled a
     WHERE
       a.ctid IN (SELECT ctid
                  FROM pgtq_{1}_scheduled
                  WHERE not_before < (now() at time zone 'utc'))
     RETURNING *)
  INSERT INTO pgtq_{1}_runnable(key) SELECT key FROM ready RETURNING *;
"""


class PgTq(object):

    def __init__(self, name, connection_string):
        self.name = name
        self.conn = psycopg2.connect(connection_string)

    def create_tables(self):
        sql = sql_template.format(self.name)
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
        seralised_taks = psycopg2.extras.Json({'name': task_name,
                                               'args': args,
                                               'kwargs': kwargs})
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, serialised_task)
