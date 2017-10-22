# PGTQ: the PostgreSQL-backed python task queue

**work in progress, not tested or usable (yet?)**

[![Build Status](https://travis-ci.org/DanielCollins/pgtq.svg?branch=master)](https://travis-ci.org/DanielCollins/pgtq) [![Coverage Status](https://coveralls.io/repos/github/DanielCollins/pgtq/badge.svg?branch=master)](https://coveralls.io/github/DanielCollins/pgtq?branch=master)

PGTQ is a task queue (also known as job queue) system. It can be used you
want to decouple execution of background tasks from the current thread. For
example, if you have a flask based web-app, you might want to send an email
due to some user action, but waiting for the SMTP server to report success
may block the page render for too long. Instead, you can push an item into
the task queue, and then immediately reply to the HTTP request. In the
background, a worker process will pick up the task of sending the email.

PGTQ tasks are writting in python, with PostgreSQL as the backing store.

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
`pop`:

    q.pop(self)

Usually, however, you will want to run dedicated worker processes. You have
to set these up yourself, because the handlers need to be imported in the
process or the worker is useless. You should use the worker main loop
functions to correctly extract items out of the queue, handling automatic
retries etc.:

    import worker

    if __name__ == "__main__":
        worker.main_loop(q)

`main_loop` never returns. You can daemonise this process prior to starting
the main loop, or run it in a process manager or terminal multiplexer any
other setup you like to ensure it keeps running in the background.

The name of the `Task` as stored in the database is accessable via `Task.name`.
By default the name of the handler function is used, but you can overide it
in the handler decorator:

    @q.handler(name="sum_task")
    def add_numbers(a, b):
        return a + b

This may be useful to avoid name conflicts.
