"""Wrapper for functions that know how to execute tasks."""


class Handler(object):
    """A callable that can execute a single type of task."""

    def __init__(self, queue, procedure, name):
        """Create a Handler for a given queue and procedure."""
        self.queue = queue
        self.procedure = procedure
        self.name = name

    def push(self, *args, **kwargs):
        """Push a task into the queue with the given arguments.

        When a worker is free it should invoke this handler on the
        task with the given arguments, in it's own thread.
        """
        self.queue.push(self.name, args, kwargs)

    def __call__(self, *args, **kwargs):
        """Call the underlying procedure of the handler."""
        return self.procedure(*args, **kwargs)
