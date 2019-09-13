EXIT_CODES = {
    "OK": 0,
    "USAGE": 64,
    "DATAERR": 65,
    "NOINPUT": 66,
    "NOUSER": 67,
    "NOHOST": 68,
    "UNAVAILABLE": 69,
    "SOFTWARE": 70,
    "OSERR": 71,
    "OSFILE": 72,
    "CANTCREAT": 73,
    "IOERR": 74,
    "TEMPFAIL": 75,
    "PROTOCOL": 76,
    "NOPERM": 77,
    "CONFIG": 78,
    "SIGINT": 130,
}


class FFError(Exception):
    """ Base class for all module exceptions.

    Subclasses just need to change the ecode.
    """

    ecode = 1

    def __init__(self, msg):
        self.msg = msg
        return


class FastaHeaderError(FFError):
    ecode = EXIT_CODES["DATAERR"]


class EmptySequenceError(FFError):
    ecode = EXIT_CODES["DATAERR"]


class FFOrderError(FFError):
    ecode = EXIT_CODES["DATAERR"]


class FFKeyError(FFError):
    ecode = 10


class InvalidOptionError(FFError):
    ecode = 1
