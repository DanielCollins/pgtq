# PGTQ: the PostgreSQL-backed python task queue

[![Build Status](https://travis-ci.org/DanielCollins/pgtq.svg?branch=master)](https://travis-ci.org/DanielCollins/pgtq) [![Coverage Status](https://coveralls.io/repos/github/DanielCollins/pgtq/badge.svg?branch=master)](https://coveralls.io/github/DanielCollins/pgtq?branch=master)

## Quickstart

Needs PostreSQL 9.5 or later installed.

Create a database:

    createdb q

Create a task queue:

    import pgtq
    
     
    q = pgtq.PgTq('test_queue', "dbname=q user=postgres") 

