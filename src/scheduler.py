#!/usr/bin/env python
"""Scheduler moves scheduled tasks to runnable at the correct time."""

import argparse
import pgtq

MIN_SLEEP_SECONDS = 0.5

description = "move scheduled or failed tasks onto the task queue"
parser = argparse.ArgumentParser(description=description)

parser.add_argument('queue', type=str,
                    help='the queue name')
parser.add_argument('connection', type=str,
                    help='connection string to database')


def schedule(queue):
    """Wait for scheduled tasks to be due, then move them into the queue.

    Never returns.
    """
    while True:
        next_wakeup = 0
        while next_wakeup and next_wakeup < MIN_SLEEP_SECONDS:
            next_wakeup = queue.run_scheduled()
            if next_wakeup:
                next_wakeup /= 1000.0
        queue.wait_for_a_schedule(next_wakeup)


def main():
    """Parse command line arguments and start scheduling."""
    args = parser.parse_args()
    q = pgtq.PgTq(args.queue, args.connection)
    schedule(q)


if __name__ == "__main__":
    main()
