# PGTQ: the PostgreSQL-backed python task queue

[![Build Status](https://travis-ci.org/DanielCollins/pgtq.svg?branch=master)](https://travis-ci.org/DanielCollins/pgtq) [![Coverage Status](https://coveralls.io/repos/github/DanielCollins/pgtq/badge.svg?branch=master)](https://coveralls.io/github/DanielCollins/pgtq?branch=master)

## Quickstart

Needs PostreSQL 9.5 or later installed.

Create a database:

    createdb q

Create a task queue:

    import pgtq
    
     
    q = pgtq.PgTq('test_queue', "dbname=q user=postgres") 

A *Handler* is a function that can perform the work of completing
a given *task*. You can create one with the `handler` decorator provided
by the queue:

    @q.handler()
    def add_numbers(a, b):
        return a + b

`add_numbers` is now a Handler, but it still can be called
directly. This will run immediately, blocking the current thread, and
without going through the task queue:

    add_numbers(2, 3)

Alternatively, you can push a *task* into the queue:

    add_numbers.push(2, 3)

This will return immediately without computing the result. As soon as possible,
a worker process should remove the task from the queue and process it. The
arguments must be anything JSON serialisable.

A `Task` can be fetched out of the queue (e.g. in a worker process) using
`get_a_task`:

    q.get_a_task(self)
