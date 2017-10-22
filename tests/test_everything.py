"""Test whether task queues work."""
import pytest
import psycopg2
import testing.postgresql
import pgtq


@pytest.fixture()
def db():
    """Yield a handle to a temporary Postgres database."""
    pg = testing.postgresql.Postgresql()
    yield pg
    pg.stop()


def test_can_make_queue(db):
    """Test whether a task queue can be created."""
    pgtq.PgTq('q', db.url())


def test_can_make_handler(db):
    """Test whether a handler can be added to a task queue."""
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    # pylint: disable=unused-variable
    def test_handler():
        """Compute the meaning of life, the universe, etc."""
        return 42


def test_can_call_handler_directly(db):
    """Test whether handlers can bypass the task queue."""
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    def test_handler(a, b):
        """Sum numbers."""
        return a + b

    assert test_handler(2, 3) == 5


def test_most_things(db):
    """Test putting a task onto the queue and getting it out again."""
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    def test_handler(a, b):
        """Sum numbers"""
        return a + b

    test_handler.push(2, 3)

    conn = psycopg2.connect(db.url())
    sql = "SELECT count(*) FROM pgtq_q_runnable;"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            assert cur.fetchone()[0] == 1

    task = q.pop()
    assert task.key == 1
    assert task.name == 'test_handler'
    assert task.retried == 0

    no_task = q.pop()
    assert no_task is None


def test_task_name_overide(db):
    """Test whether we can override the default name of a task."""
    q = pgtq.PgTq('q', db.url())

    @q.handler(name='more_awesome_name')
    def test_handler(a, b):
        """Sum numbers"""
        return a + b

    test_handler.push(2, 3)
    task = q.pop()
    assert task.name == 'more_awesome_name'
