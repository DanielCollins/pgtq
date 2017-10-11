DROP TABLE IF EXISTS scheduled;
DROP TABLE IF EXISTS runnable;
DROP TABLE IF EXISTS running;
DROP TABLE IF EXISTS complete;
DEALLOCATE ALL;

CREATE TABLE scheduled (
  key INTEGER PRIMARY KEY,
  not_before TIMESTAMP WITHOUT TIME ZONE,
  task JSON,
  retried INTEGER
);

CREATE INDEX CONCURRENTLY ON scheduled (not_before);

CREATE TABLE runnable (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE running (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

CREATE TABLE complete (
  key INTEGER PRIMARY KEY,
  task JSON,
  retried INTEGER
);

PREPARE lock_task AS
  WITH task
  AS (DELETE FROM runnable a
      WHERE a.ctid = (SELECT ctid
                      FROM runnable
                      ORDER BY key
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
      RETURNING *)
    INSERT INTO running SELECT * FROM task RETURNING *;

PREPARE mark_completed (INTEGER) AS
  WITH task
  AS (DELETE FROM running WHERE key=$1 RETURNING *)
  INSERT INTO complete SELECT * FROM task RETURNING *;

PREPARE mark_interupted(INTEGER) AS
  WITH task
  AS (DELETE FROM running WHERE key=$1 RETURNING *)
  INSERT INTO scheduled
    SELECT key,
           (now() at time zone 'utc') + 
            (INTERVAL '1 second' * random() * retried),
           task,
           (retried + 1)
    FROM task RETURNING *;

PREPARE run_scheduled AS
  WITH ready AS
    (DELETE FROM scheduled a
     WHERE
       a.ctid IN (SELECT ctid
                  FROM scheduled
                  WHERE not_before < (now() at time zone 'utc'))
     RETURNING *)
  INSERT INTO runnable(key) SELECT key FROM ready RETURNING *;
