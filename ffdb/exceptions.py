class FFKeyError(Exception):

    def __init__(self, msg):
        self.msg = msg
        self.ecode = 10
        return


class InvalidOptionError(Exception):

    def __init__(self, msg):
        self.msg = msg
        self.ecode = 1
        return
