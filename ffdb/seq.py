""" Simple fasta parser and utilities. """

from collections.abc import Iterator


class Seq(object):

    def __init__(self, id, desc, seq):
        """ Construct a new Seq object.
        Keyword arguments:
        id -- The sequence id <str>.
        desc -- A short description of the sequence <str>.
        seq -- The biological sequence <str>.
        """
        self.id = id
        self.desc = desc
        self.seq = seq
        return

    def __str__(self):
        """ Returns a FASTA string from the object """
        line_length = 60

        if self.desc is None:
            lines = [">{}".format(self.id)]
        else:
            lines = [">{} {}".format(self.id, self.desc)]

        for i in range(0, len(self), line_length):
            lines.append(self.seq[i:i+line_length].decode("utf-8"))

        return "\n".join(lines) + "\n"

    def __repr__(self):
        """ Returns a simple string representation of the object. """
        cls = self.__class__.__name__
        return "{}(id='{}', desc='{}', seq='{}')".format(cls, self.id,
                                                         self.desc, self.seq)

    def __getitem__(self, key):
        """ Allow us to access indices from the seq directly. """
        seq = self.seq[key]
        return self.__class__(self.id, self.desc, seq)

    def __eq__(self, other):
        """ Allows us to compare two Seq objects directly using '==' .
        NB. python internally implements != based on this too.
        """
        if isinstance(other, self.__class__):
            return self.seq == other.seq
        elif isinstance(other, str):
            return self.seq == other
        else:
            raise ValueError((
                "Equality comparisons not implemented between {} and {}."
                ).format(type(self), type(other)))
        return

    def __len__(self):
        """ The length of a Seq object should be the length of the seq. """
        return len(self.seq)

    @classmethod
    def read(cls, handle):
        """ Read a single FASTA record.
        Parses a single FASTA record into a Seq object.
        Assumes that the first line will always contain the id line,
        and that there is a single FASTA sequence.
        Keyword arguments:
        handle -- An iterable containing lines (newline split) of the file.
        Returns:
        A Seq object.
        """
        if not isinstance(handle, Iterator):
            handle = iter(handle)

        try:
            id_, desc = cls._split_id_line(next(handle).strip())
        except ValueError as e:
            raise ValueError("Fasta parsing failed. " + str(e))

        # tuple comprehensions are generators so we're still doing lazy eval
        seq = "".join((l.strip() for l in handle))
        return Seq(id_, desc, seq.encode())

    @classmethod
    def parse(cls, handle):
        """ Parse multiple fasta records.
        Parses a multi-fasta formatted file-like object.
        Keyword arguments:
        handle -- A file-like object or any iterable over the fasta file lines.
        Yields:
        Seq objects.
        """

        # Store the initial state to avoid outputting empty record.
        first = True
        # Store lines for this block here.
        current_record = []

        for line in handle:
            if line.startswith(">"):
                if not first:
                    # Yield makes this function a generator.
                    # NB we reuse the read method to avoid repetition.
                    # It's also easier to test.
                    yield cls.read(current_record)
                else:
                    # Once we've passed the first sequence this passes.
                    first = False

                # Start a new block
                current_record = [line]
            else:
                # For lines containing sequences we simply append the sequence.
                current_record.append(line)

        # The last sequence in the file won't have a ">" following it.
        # so we yield the last block too.
        yield cls.read(current_record)
        return

    @classmethod
    def parse_many(cls, handles):
        for handle in handles:
            for record in cls.parse(handle):
                yield record
        return

    @staticmethod
    def _split_id_line(line):
        """ Parse the FASTA header line into id and description components.
        NB expects the '>' character to be present at start of line.
        Keyword arguments:
        line -- A string containing the header.
        Returns:
        Tuple -- id and description strings.
        """

        if not line.startswith(">"):
            raise ValueError(("Encountered malformed fasta header. "
                              "Offending line is '{}'").format(line))
        # Strip the ">" character and split at most 1 time on spaces.
        sline = line[1:].split(" ", 1)

        if len(sline) == 1:
            return sline[0], None
        else:
            return sline[0], sline[1]

        # We should never reach this point.
        return

    def checksum(self):
        """ Returns the seguid checksum of a sequence """
        from hashlib import sha1
        from base64 import b64encode
        hash_ = sha1(self.seq).digest()
        return b64encode(hash_).rstrip(b"=").decode("utf-8")
