""" Simple fasta parser and utilities. """

from collections.abc import Iterator
from ffdb.exceptions import FastaHeaderError, EmptySequenceError


class Seq(object):

    def __init__(self, id, desc, seq):
        """ Construct a new Seq object.

        Keyword arguments:
        id -- The sequence id <str>.
        desc -- A short description of the sequence <str>.
        seq -- The biological sequence <bytes>.

        Examples:
        >>> Seq("test", "description", "ATGCA")
        Seq(id='test', desc='description', seq=b'ATGCA')
        """

        self.id = id
        self.desc = desc

        # Seq should be bytes
        if isinstance(seq, str):
            seq = seq.encode()

        self.seq = seq
        return

    def __str__(self):
        """ Returns a FASTA string from the object.

        Examples:
        >>> print(Seq("test", "description", "ATGCA".encode()))
        >test description
        ATGCA
        """
        line_length = 60

        if self.desc is None:
            lines = [">{}".format(self.id)]
        else:
            lines = [">{} {}".format(self.id, self.desc)]

        for i in range(0, len(self), line_length):
            lines.append(self.seq[i:i+line_length].decode("utf-8"))

        return "\n".join(lines)

    def __repr__(self):
        """ Returns a simple string representation of the object. """

        cls = self.__class__.__name__
        repr_id = repr(self.id)
        repr_desc = repr(self.desc)
        repr_seq = repr(self.seq)

        return f"{cls}(id={repr_id}, desc={repr_desc}, seq={repr_seq})"

    def __getitem__(self, key):
        """ Allow us to access indices from the seq directly.

        Examples:
        >>> seq = Seq("test", "description", "ATGCA".encode())
        >>> seq[0]
        Seq(id='test', desc='description', seq=b'A')
        """

        # Keep the output as a byteslice even when selecting single character.
        if isinstance(key, int):
            key = slice(key, key + 1)

        seq = self.seq[key]
        return self.__class__(self.id, self.desc, seq)

    def __eq__(self, other):
        """ Allows us to compare two Seq objects directly using '==' .
        NB. python internally implements != based on this too.

        Examples:
        >>> seq = Seq("test", "description", "ATGCA".encode())
        >>> assert seq == Seq("test2", "description", "ATGCA".encode())
        >>> assert seq == "ATGCA"
        """

        if isinstance(other, self.__class__):
            return self.seq == other.seq
        elif isinstance(other, bytes):
            return self.seq == other
        elif isinstance(other, str):
            return self.seq == other.encode()
        else:
            raise ValueError((
                "Equality comparisons not implemented between {} and {}."
                ).format(type(self), type(other)))
        return

    def __len__(self):
        """ The length of a Seq object should be the length of the seq.

        Examples:
        >>> seq = Seq("test", "description", "ATGCA")
        >>> len(seq)
        5
        """

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

        Examples:
        >>> fasta = [">test description", "ATGCA"]
        >>> Seq.read(fasta)
        Seq(id='test', desc='description', seq=b'ATGCA')
        """

        if not isinstance(handle, Iterator):
            handle = iter(handle)

        line = next(handle).strip()

        try:
            id_, desc = cls._split_id_line(line)
        except ValueError:
            raise FastaHeaderError(
                "Encountered malformed fasta header. "
                f"Offending line is: '{line}'"
            )

        # tuple comprehensions are generators so we're still doing lazy eval
        seq = "".join((l.strip() for l in handle))

        if seq == "":
            raise EmptySequenceError(
                f"Record with id {id_} has no sequence data. "
                f"This could be a truncated file or a non-standard format."
            )

        return Seq(id_, desc, seq.encode())

    @classmethod
    def parse(cls, handle):
        """ Parse multiple fasta records.

        Parses a multi-fasta formatted file-like object.

        Keyword arguments:
        handle -- A file-like object or any iterable over the fasta file lines.

        Yields:
        Seq objects.

        Examples:
        >>> fasta = [
        ...     ">test1 description",
        ...     "ATGCA",
        ...     ">test2 descr",
        ...     "TGACA",
        ... ]
        >>> seqs = Seq.parse(fasta)
        >>> next(seqs)
        Seq(id='test1', desc='description', seq=b'ATGCA')
        >>> next(seqs)
        Seq(id='test2', desc='descr', seq=b'TGACA')
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
        """ Parses many files yielding an iterator over all of them. """

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

        Examples:
        >>> Seq._split_id_line(">one two")
        ('one', 'two')
        >>> Seq._split_id_line(">one ")
        ('one', '')
        >>> Seq._split_id_line(">one")
        ('one', None)
        """

        if not line.startswith(">"):
            raise ValueError()

        # Strip the ">" character and split at most 1 time on spaces.
        sline = line[1:].split(" ", 1)

        if len(sline) == 1:
            return sline[0], None
        else:
            return sline[0], sline[1]

        # We should never reach this point.
        return
