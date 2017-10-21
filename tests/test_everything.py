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
