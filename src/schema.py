"""SQL for storing and working on tasks."""

SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS pgtq_{0}_scheduled (
  key INTEGER PRIMARY KEY,
  not_before TIMESTAMP WITHOUT TIME ZONE,
  task JSON,
  retried INTEGER,
  max_retries INTEGER
);

CREATE INDEX IF NOT EXISTS
  ix_pgtq_{0}_scheduled_not_before ON pgtq_{0}_scheduled (not_before);

CREATE TABLE IF NOT EXISTS pgtq_{0}_runnable (
  key SERIAL PRIMARY KEY,
  task JSON,
  retried INTEGER DEFAULT 0,
  max_retries INTEGER DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS pgtq_{0}_running (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER,
  max_retries INTEGER
);

CREATE TABLE IF NOT EXISTS pgtq_{0}_complete (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER,
  max_retries INTEGER
);

CREATE TABLE IF NOT EXISTS pgtq_{0}_failed (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER,
  max_retries INTEGER
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

PREPARE pgtq_{0}_move_scheduled AS
  WITH ready AS
    (DELETE FROM pgtq_{0}_scheduled a
     WHERE
       a.ctid IN (SELECT ctid
                  FROM pgtq_{0}_scheduled
                  WHERE not_before < (now() at time zone 'utc'))
     RETURNING *)
  INSERT INTO pgtq_{0}_runnable(key) SELECT key FROM ready RETURNING *;

CREATE FUNCTION pgtq_{0}_run_scheduled () RETURNS
   TIMESTAMP WITHOUT TIME ZONE AS $$
    EXECUTE pgtq_{0}_move_scheduled;
    SELECT MIN(not_before) from pgtq_{0}_scheduled;
$$ LANGUAGE SQL;

CREATE FUNCTION pgtq_{0}_push(in_task JSON) RETURNS void AS $$
BEGIN
    INSERT INTO pgtq_{0}_runnable (task) VALUES (in_task);
    NOTIFY pgtq_{0}_runnable_channel;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgtq_{0}_interupt(in_key INTEGER) RETURNS void AS $$
DECLARE
  task pgtq_{0}_running%ROWTYPE;
BEGIN
   DELETE FROM pgtq_{0}_running WHERE key=in_key RETURNING * INTO task;
   IF task.max_retries IS NULL OR task.retried < task.max_retries THEN
      INSERT INTO pgtq_{0}_scheduled (key, not_before, task, retried,
                                      max_retries)
      VALUES (task.key,
              (now() at time zone 'utc') +
                (INTERVAL '1 second' * random() * task.retried),
              task.task,
              (task.retried + 1),
              task.max_retries);
    ELSE
      INSERT INTO pgtq_{0}_failed (key, task, retries, max_retries)
      VALUES (task.key,
              task.task,
              task.retries,
              task.max_retries);
    END IF;

    NOTIFY pgtq_{0}_scheduled_channel;
END;
$$ LANGUAGE plpgsql;
"""
