"""Tasks are the work items stored in the queue."""


class Task(object):
    """Represents a single task."""

    def __init__(self, json_repr):
        """Create a task from its' serialised JSON representation."""
        self.key = json_repr[0]
        self.name = json_repr[1]['name']
        self.args = json_repr[1]['args']
        self.retried = json_repr[2]
