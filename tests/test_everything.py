import pytest
import psycopg2
import testing.postgresql
import pgtq


@pytest.fixture()
def db():
    pg = testing.postgresql.Postgresql()
    yield pg
    pg.stop()


def test_can_make_queue(db):
    pgtq.PgTq('q', db.url())


def test_can_make_handler(db):
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    # pylint: disable=unused-variable
    def test_handler():
        return 42


def test_can_call_handler_directly(db):
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    def test_handler(a, b):
        return a + b

    assert test_handler(2, 3) == 5


def test_can_push_task(db):
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    def test_handler(a, b):
        return a + b

    test_handler.push(2, 3)

    conn = psycopg2.connect(db.url())
    sql = "SELECT count(*) FROM pgtq_q_runnable;"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            assert cur.fetchone()[0] == 1
