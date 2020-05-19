""" Simple fasta parser and utilities. """

from typing import Optional, Union, Any
from typing import Sequence, List, Iterable, Iterator
from typing import Tuple

from ffdb.exceptions import FastaHeaderError, EmptySequenceError


class Seq(object):

    def __init__(self, id: str, desc: Optional[str], seq: bytes):
        """ Construct a new Seq object.

        Keyword arguments:
        id -- The sequence id <str>.
        desc -- A short description of the sequence <str>.
        seq -- The biological sequence <bytes>.

        Examples:
        >>> Seq("test", "description", b"ATGCA")
        Seq(id='test', desc='description', seq=b'ATGCA')
        """

        self.id = id
        self.desc = desc

        self.seq = seq
        return

    def __str__(self) -> str:
        """ Returns a FASTA string from the object.

        Examples:
        >>> print(Seq("test", "description", b"ATGCA"))
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

    def __bytes__(self) -> bytes:
        """ Returns a FASTA bytestring from the object.

        Examples:
        >>> bytes(Seq("test", "description", b"ATGCA"))
        b'>test description\\nATGCA'
        """
        line_length = 60

        if self.desc is None:
            lines = [">{}".format(self.id).encode("utf-8")]
        else:
            lines = [">{} {}".format(self.id, self.desc).encode("utf-8")]

        for i in range(0, len(self), line_length):
            lines.append(self.seq[i:i+line_length])

        return b"\n".join(lines)

    def __repr__(self) -> str:
        """ Returns a simple string representation of the object. """

        cls = self.__class__.__name__
        repr_id = repr(self.id)
        repr_desc = repr(self.desc)
        repr_seq = repr(self.seq)

        return f"{cls}(id={repr_id}, desc={repr_desc}, seq={repr_seq})"

    def __getitem__(self, key: Union[int, slice]) -> "Seq":
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

    def __eq__(self, other: Any) -> bool:
        """ Allows us to compare two Seq objects directly using '==' .
        NB. python internally implements != based on this too.

        Examples:
        >>> seq = Seq("test", "description", "ATGCA".encode())
        >>> assert seq == Seq("test2", "description", "ATGCA".encode())
        >>> assert seq == b"ATGCA"
        """

        if isinstance(other, self.__class__):
            return self.seq == other.seq
        elif isinstance(other, bytes):
            return self.seq == other
        else:
            raise ValueError((
                "Equality comparisons not implemented between {} and {}."
                ).format(type(self), type(other)))

    def __len__(self) -> int:
        """ The length of a Seq object should be the length of the seq.

        Examples:
        >>> seq = Seq("test", "description", b"ATGCA")
        >>> len(seq)
        5
        """

        return len(self.seq)

    @classmethod
    def read(cls, handle: Iterable[bytes]) -> "Seq":
        """ Read a single FASTA record.

        Parses a single FASTA record into a Seq object.
        Assumes that the first line will always contain the id line,
        and that there is a single FASTA sequence.

        Keyword arguments:
        handle -- An iterable containing lines (newline split) of the file.

        Returns:
        A Seq object.

        Examples:
        >>> fasta = [b">test description", b"ATGCA"]
        >>> Seq.read(fasta)
        Seq(id='test', desc='description', seq=b'ATGCA')
        """

        ihandle = iter(handle)

        try:
            line = next(ihandle).strip()
        except StopIteration:
            raise FastaHeaderError(
                "The sequence that we're trying to parse was empty."
            )

        try:
            id_, desc = cls._split_id_line(line)
        except ValueError:
            raise FastaHeaderError(
                "Encountered malformed fasta header. "
                f"Offending line is: '{line.decode()}'"
            )

        # tuple comprehensions are generators so we're still doing lazy eval
        seq = b"".join((l.strip() for l in ihandle))

        if seq == "":
            raise EmptySequenceError(
                f"Record with id {id_} has no sequence data. "
                f"This could be a truncated file or a non-standard format."
            )

        return Seq(id_, desc, seq)

    @classmethod
    def parse(cls, handle: Sequence[bytes]) -> Iterator["Seq"]:
        """ Parse multiple fasta records.

        Parses a multi-fasta formatted file-like object.

        Keyword arguments:
        handle -- A file-like object or any iterable over the fasta file lines.

        Yields:
        Seq objects.

        Examples:
        >>> fasta = [
        ...     b">test1 description",
        ...     b"ATGCA",
        ...     b">test2 descr",
        ...     b"TGACA",
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
        current_record: List[bytes] = []

        for line in handle:
            if line.startswith(b">"):
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
        if len(current_record) > 0:
            yield cls.read(current_record)
        return

    @classmethod
    def parse_many(cls, handles: Sequence[Sequence[bytes]]) -> Iterator["Seq"]:
        """ Parses many files yielding an iterator over all of them. """

        for handle in handles:
            for record in cls.parse(handle):
                yield record
        return

    @staticmethod
    def _split_id_line(line: bytes) -> Tuple[str, Optional[str]]:
        """ Parse the FASTA header line into id and description components.
        NB expects the '>' character to be present at start of line.

        Keyword arguments:
        line -- A string containing the header.

        Returns:
        Tuple -- id and description strings.

        Examples:
        >>> Seq._split_id_line(b">one two")
        ('one', 'two')
        >>> Seq._split_id_line(b">one ")
        ('one', '')
        >>> Seq._split_id_line(b">one")
        ('one', None)
        """

        if not line[:1] == b">":
            raise ValueError()

        # Strip the ">" character and split at most 1 time on spaces.
        sline = line[1:].decode("utf-8").split(" ", 1)

        if len(sline) == 1:
            return sline[0], None
        else:
            return sline[0], sline[1]
