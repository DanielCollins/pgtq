import psycopg2

sql = """
CREATE TABLE pgtq_scheduled (
  key INTEGER PRIMARY KEY,
  not_before TIMESTAMP WITHOUT TIME ZONE,
  task JSON,
  retried INTEGER
);

CREATE INDEX CONCURRENTLY ON pgtq_scheduled (not_before);

CREATE TABLE pgtq_runnable (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE pgtq_running (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE pgtq_complete (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

PREPARE pgtq_lock_task AS
  WITH task
  AS (DELETE FROM pgtq_runnable a
      WHERE a.ctid = (SELECT ctid
                      FROM pgtq_runnable
                      ORDER BY key
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
      RETURNING *)
    INSERT INTO pgtq_running SELECT * FROM task RETURNING *;

PREPARE pgtq_mark_completed (INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_complete SELECT * FROM task RETURNING *;

PREPARE pgtq_mark_interupted(INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_scheduled
    SELECT key,
           (now() at time zone 'utc') +
            (INTERVAL '1 second' * random() * retried),
           task,
           (retried + 1)
    FROM task RETURNING *;

PREPARE pgtq_run_scheduled AS
  WITH ready AS
    (DELETE FROM pqtq_scheduled a
     WHERE
       a.ctid IN (SELECT ctid
                  FROM pgtq_scheduled
                  WHERE not_before < (now() at time zone 'utc'))
     RETURNING *)
  INSERT INTO pgtq_runnable(key) SELECT key FROM ready RETURNING *;
"""

class PgTq(object):

    def __init__(self, name, connection_string):
        self.name = name
        self.conn = psycopg2.connect(connection_string)

    def create_tables(self):
        with self.conn:
            with conn.cursor() as cursor:
                cur.execute(sql)
