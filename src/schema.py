"""SQL for storing and working on tasks."""

SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS pgtq_{0}_scheduled (
  key INTEGER PRIMARY KEY,
  not_before TIMESTAMP WITHOUT TIME ZONE,
  task JSON,
  retried INTEGER
);

CREATE INDEX IF NOT EXISTS
  ix_pgtq_{0}_scheduled_not_before ON pgtq_{0}_scheduled (not_before);

CREATE TABLE IF NOT EXISTS pgtq_{0}_runnable (
  key SERIAL PRIMARY KEY,
  task JSON,
  retried INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pgtq_{0}_running (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE IF NOT EXISTS pgtq_{0}_complete (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

PREPARE pgtq_{0}_lock_task AS
  WITH task
  AS (DELETE FROM pgtq_{0}_runnable a
      WHERE a.ctid = (SELECT ctid
                      FROM pgtq_{0}_runnable
                      ORDER BY key
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
      RETURNING *)
    INSERT INTO pgtq_{0}_running SELECT * FROM task RETURNING *;

PREPARE pgtq_{0}_mark_completed (INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_{0}_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_{0}_complete SELECT * FROM task RETURNING *;

PREPARE pgtq_{0}_mark_interupted(INTEGER) AS
  WITH task
  AS (DELETE FROM pgtq_{0}_running WHERE key=$1 RETURNING *)
  INSERT INTO pgtq_{0}_scheduled
    SELECT key,
           (now() at time zone 'utc') +
            (INTERVAL '1 second' * random() * retried),
           task,
           (retried + 1)
    FROM task RETURNING *;

PREPARE pgtq_{0}_run_scheduled AS
  WITH ready AS
    (DELETE FROM pgtq_{0}_scheduled a
     WHERE
       a.ctid IN (SELECT ctid
                  FROM pgtq_{0}_scheduled
                  WHERE not_before < (now() at time zone 'utc'))
     RETURNING *)
  INSERT INTO pgtq_{0}_runnable(key) SELECT key FROM ready RETURNING *;
"""
