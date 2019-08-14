class FFError(Exception):
    """ Base class for all module exceptions.

    Subclasses just need to change the ecode.
    """

    ecode = 1

    def __init__(self, msg):
        self.msg = msg
        return


class FFKeyError(FFError):
    ecode = 10


class InvalidOptionError(FFError):
    ecode = 1
