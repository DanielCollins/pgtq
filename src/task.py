"""Tasks are the work items stored in the queue."""


class Task(object):
    """Represents a single task."""

    def __init__(self, queue, json_repr):
        """Create a task from its' serialised JSON representation."""
        self.queue = queue
        self.key = json_repr[0]
        self.name = json_repr[1]['name']
        self.args = json_repr[1]['args']
        self.kwargs = json_repr[1]['kwargs']
        self.retried = json_repr[2]

    def get_handler(self):
        """Return a `Handler` capable of executing this `Task`."""
        return self.queue.handlers[self.name]

    def execute(self):
        """Find and execute the handler for this task, completing it.

        If the handler fails to execute on time, retry it up to the
        maximum number of times. If it still fails, mark it permanently
        failed.
        """
        handler = self.get_handler()
        return handler(*self.args, **self.kwargs)
