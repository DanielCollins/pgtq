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
