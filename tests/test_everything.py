import pytest
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
    def test_handler():
        return 42


def test_can_call_handler_directly(db):
    q = pgtq.PgTq('q', db.url())

    @q.handler()
    def test_handler(a, b):
        return a + b

    assert test_handler(2, 3) == 5
