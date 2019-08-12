""" A generator that gives lexicographically orderable and selectable ids
based on an integer """


class IdConverter(object):

    """
    Generates an id based on an integer.
    Essentially it's a small hashing algorithm that allows us to
    give unique ids that aren't too long.
    """

    def __init__(
            self,
            state: int = 0,
            prefix: str = "",
            length: int = 4,
            alphabet: str = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ',
            ) -> None:
        """ Create a new id converter with a given state and alphabet. """

        self.state = state
        self.prefix = prefix
        self.length = length
        self.alphabet = alphabet

        from baseconv import BaseConverter
        self.converter = BaseConverter(alphabet)
        return

    def encode(self, number: int) -> str:
        """ Given an integer get the string representation. """

        template = "{pre}{{p:{first}>{length}}}".format(
            pre=self.prefix,
            first=self.converter.digits[0],
            length=self.length,
            )
        return template.format(p=self.converter.encode(number))

    def decode(self, pattern: str) -> int:
        """ Given a string representation get the integer that produced it. """

        pattern = pattern[len(self.prefix):]
        return int(self.converter.decode(pattern))

    def __next__(self):
        string = self.encode(self.state)
        self.state += 1
        return string

    def __iter__(self):
        return self
