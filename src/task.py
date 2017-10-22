class Task(object):

    def __init__(self, json_repr):
        self.key = json_repr[0]
        self.name = json_repr[1]['name']
        self.args = json_repr[1]['args']
        self.retried = json_repr[2]
