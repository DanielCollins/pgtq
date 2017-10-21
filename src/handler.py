class Handler(object):

    def __init__(self, queue, procedure, name=None):
        self.queue = queue
        self.procedure = procedure
        if not name:
            name = procedure.__name__
        self.name = name

    def push(self, *args, **kwargs):
        self.queue.push(self.name, args, kwargs)

    def __call__(self, *args, **kwargs):
        return self.procedure(*args, **kwargs)
