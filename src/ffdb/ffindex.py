""" Classes for reading and writing ffindex databases. """

from os.path import split as psplit
from os import makedirs
from copy import deepcopy
from shutil import copyfileobj
from io import BytesIO

from typing import NamedTuple, Tuple
from typing import Sequence, Iterator, List
from typing import Dict
from typing import BinaryIO

from typing import Union, Optional


class IndexRow(NamedTuple):

    name: bytes
    start: int
    size: int

    @classmethod
    def parse_ffindex_line(cls, line: bytes) -> "IndexRow":
        """ Parse a line from a .ffindex file.

        Examples:
        >>> IndexRow.parse_ffindex_line(b"one\t0\t50")
        IndexRow(name=b'one', start=0, size=50)
        """

        name, start, size = line.strip().split()
        row = cls(
            name,
            int(start),
            int(size)
        )  # size - 1?
        return row

    def __str__(self):
        return "{}\t{}\t{}".format(
            self.name.decode("utf-8"),
            self.start,
            self.size
        )

    def __bytes__(self):
        return b'\t'.join([
            self.name,
            str(self.start).encode("utf-8"),
            str(self.size).encode("utf-8")
        ])


class FFIndex(object):

    def __init__(self, index: Optional[Sequence[IndexRow]] = None) -> None:
        """ Construct an ffindex given a list of index rows. """

        if index is None:
            index = []
        else:
            assert all(isinstance(r, IndexRow) for r in index)

        self.index: List[IndexRow] = sorted(index, key=lambda x: x.start)
        self.lookup: Dict[bytes, IndexRow] = dict()

        for idx in self.index:
            # A well formed ffindex should never have duplicate names.
            assert idx.name not in self.lookup
            self.lookup[idx.name] = idx

        return

    def __getitem__(
        self,
        key: Union[bytes, slice, int]
    ) -> Union[IndexRow, List[IndexRow]]:

        if isinstance(key, (int, slice)):
            return self.index[key]
        elif isinstance(key, bytes):
            return self.lookup[key]
        else:
            raise ValueError(
                "Expected either a bytes, an int, or a slice."
            )

    def __contains__(self, key: bytes) -> bool:
        return key in self.lookup

    def __iter__(self) -> Iterator[IndexRow]:
        for row in self.index:
            yield row
        return

    def __len__(self) -> int:
        return len(self.index)

    @classmethod
    def from_file(cls, handle: BinaryIO) -> "FFIndex":
        indices = list()

        for line in handle:
            row = IndexRow.parse_ffindex_line(line)
            indices.append(row)

        return cls(index=indices)

    def append(self, value: IndexRow) -> None:
        assert isinstance(value, IndexRow)

        name, start, size = value

        assert name not in self

        if len(self.index) > 0:
            last_row = self.index[-1]
            last_end = last_row.start + last_row.size
            start = last_end
        else:
            start = 0

        new_record = IndexRow(name, start, size)
        self.index.append(new_record)

        self.lookup[name] = new_record
        return

    def extend(self, values: Sequence[IndexRow]) -> int:
        for value in values:
            self.append(value)
        return len(values)

    def write_to(self, handle: BinaryIO) -> int:
        length = 0
        for ind in sorted(self.index, key=lambda x: x.name):
            line = "{}\t{}\t{}\n".format(
                ind.name.decode("utf-8"),
                ind.start,
                ind.size
            )
            length += handle.write(line.encode())

        return length

    def bump_starts(self, by: int = 0):
        if by == 0:
            return deepcopy(self)

        new_index: List[IndexRow] = []
        for name, start, size in self.index:
            new_index.append(IndexRow(name, start + by, size))

        # Construct a new object
        return self.__class__(new_index)


class FFData(object):

    def __init__(self, handle: BinaryIO) -> None:
        self.handle = handle
        return

    def __getitem__(
        self,
        key: Union[IndexRow, List[IndexRow]]
    ) -> Union[bytes, List[bytes]]:

        if isinstance(key, IndexRow):
            name, start, size = key
            self.handle.seek(start)
            return self.handle.read(size)

        elif isinstance(key, list):
            records = []
            for name, start, size in key:
                self.handle.seek(start)
                records.append(self.handle.read(size))
            return records

        else:
            raise ValueError("Must be an IndexRow or a list of IndexRows")

    def append(self, b: bytes) -> int:
        self.handle.seek(0, 2)  # Go to end of file.
        assert b[-1:] == b"\0"
        return self.handle.write(b)

    def write_to(self, handle: BinaryIO) -> None:
        self.handle.seek(0)
        copyfileobj(self.handle, handle)
        return

    def write_sized(self, start: int, size: int, handle: BinaryIO) -> int:
        self.handle.seek(start)
        return handle.write(self.handle.read(size))


class FFDB(object):

    def __init__(self, data: FFData, index: FFIndex) -> None:
        self.data: FFData = data
        self.index: FFIndex = index
        return

    @classmethod
    def from_file(
        cls,
        data_handle: BinaryIO,
        index_handle: BinaryIO
    ) -> "FFDB":
        data = FFData(data_handle)
        index = FFIndex.from_file(index_handle)
        return cls(data, index)

    @classmethod
    def new(cls, data_handle: Optional[BinaryIO] = None) -> "FFDB":
        if data_handle is None:
            data_handle = BytesIO()

        data = FFData(data_handle)
        index = FFIndex()
        return cls(data, index)

    @classmethod
    def reorder_from(
        cls,
        other: "FFDB",
        data_handle: BinaryIO = None,
        order: Optional[Sequence[IndexRow]] = None,
    ) -> "FFDB":

        new = cls.new(data_handle)

        if order is None:
            indices: List[IndexRow] = sorted(
                other.index.index,
                key=lambda i: i.size,
                reverse=True
            )
        else:
            indices = list(order)

        new.extend_from(other, indices)
        return new

    def __getitem__(
        self,
        key: Union[bytes, slice, int]
    ) -> Union[bytes, List[bytes]]:
        indices = self.index[key]
        return self.data[indices]

    def __contains__(self, key: bytes) -> bool:
        return key in self.index

    def __len__(self) -> int:
        return len(self.index)

    def append_from(
        self,
        data: "FFDB",
        key: Union[bytes, int, IndexRow]
    ) -> int:

        if isinstance(key, IndexRow):
            this_key: Union[IndexRow, List[IndexRow]] = key
        else:
            this_key = data.index[key]

        # We shouldn't ever get a list back because we didn't allow slices in
        # key.
        assert isinstance(this_key, IndexRow)

        to_write = data.data[this_key]
        assert isinstance(to_write, bytes)

        self.index.append(this_key)
        return self.data.append(to_write)

    def extend_from(
        self,
        data: "FFDB",
        keys: Union[None, slice, Sequence[Union[bytes, int, IndexRow]]]
    ) -> int:

        if isinstance(keys, slice):
            indices: Union[IndexRow, List[IndexRow]] = data.index[keys]
        elif keys is None:
            indices = data.index.index
        else:
            indices = []
            for k in keys:
                if isinstance(k, IndexRow):
                    indices.append(k)
                else:
                    ir = data.index[k]
                    assert isinstance(ir, IndexRow)
                    indices.append(ir)

        assert isinstance(indices, list)

        length = 0
        for key in indices:
            length += self.append_from(data, key)
        return length

    def append(self, data: bytes, key: bytes) -> int:
        if data[-1:] != b'\0':
            data = data + b'\0'

        self.data.append(data)
        self.index.append(IndexRow(key, 0, len(data)))
        return len(data)

    def extend(self, data: Sequence[bytes], keys: Sequence[bytes]) -> int:
        assert len(data) == len(keys)

        length = 0
        for k, d in zip(keys, data):
            length += self.append(d, k)

        return length

    def write_to(
        self,
        data_handle: BinaryIO,
        index_handle: BinaryIO
    ) -> None:
        assert data_handle.tell() == 0

        self.index.write_to(index_handle)
        self.data.write_to(data_handle)
        return

    def concat(self, dbs: Sequence["FFDB"]) -> None:
        # Go to end of file
        self.data.handle.seek(0, 2)
        for db in dbs:
            self.index.extend(db.index.index)
            db.data.write_to(self.data.handle)
        return

    def documents(
        self,
        trim: Optional[int] = None
    ) -> Iterator[Tuple[bytes, bytes]]:
        """ Iterate over all documents in a db into a single file.

        Optionally removing `trim` lines from the beginning of each
        document.
        """

        for index in self.index:

            # Take up to :-1 to strip the null byte
            document = self.data[index]
            assert isinstance(document, bytes)

            trimmed_document = document[:-1]

            if trim is not None:
                sdocument = trimmed_document.split(b'\n')[trim:]
                if len(sdocument) == 0:
                    continue

                yield index.name, b'\n'.join(sdocument)
            else:
                yield index.name, trimmed_document
        return

    def collect_into(
        self,
        outfile: BinaryIO,
        trim: Optional[int] = None
    ) -> None:
        """ Collect all documents from a db into a single file.

        Optionally removing `trim` lines from the beginning of each
        document.
        """

        for key, document in self.documents(trim=trim):
            outfile.write(document)
            if not document.endswith(b'\n'):
                outfile.write(b'\n')
        return

    def partition(
        self,
        name: str,
        order: Optional[Sequence[IndexRow]] = None,
        template: str = "{name}_{index}.{ext}",
        n: int = 10000
    ) -> int:
        """ Chunk a database into partitions of size n """
        from math import ceil

        if order is None:
            indices: List[IndexRow] = sorted(
                self.index.index,
                key=lambda i: i.size,
                reverse=True
            )
        else:
            assert len(order) == len(self.index)
            indices = list(order)

        nchunks = ceil(len(self.index) / n)
        for i in range(nchunks):
            chunk = indices[i::nchunks]

            ffindex_name = template.format(
                name=name,
                index=i + 1,
                ext="ffindex"
            )

            ffdata_name = template.format(
                name=name,
                index=i + 1,
                ext="ffdata"
            )

            index_dirname = psplit(ffindex_name)[0]
            makedirs(index_dirname, exist_ok=True)

            data_dirname = psplit(ffdata_name)[0]
            makedirs(data_dirname, exist_ok=True)

            with open(ffindex_name, "wb") as index_handle, \
                    open(ffdata_name, "wb") as data_handle:

                chunkdb = FFDB.reorder_from(
                    self,
                    data_handle=data_handle,
                    order=chunk
                )
                chunkdb.index.write_to(index_handle)

        return nchunks

    def quick_partition(
        self,
        name: str,
        template="{name}_{index}.{ext}",
        n=10000
    ) -> int:
        """ Chunk a database into partitions of size n """

        start_pos = 0
        pindices: List[IndexRow] = []
        partition = 1

        for i, p in enumerate(self.index, 1):
            if i % n == 0:
                self._write_quick_partition(
                    start_pos,
                    p.start,
                    template,
                    name,
                    pindices,
                    partition
                )

                pindices = []
                partition += 1
                start_pos = p.start

            pindices.append(p)

        if len(pindices) > 1:
            end = pindices[-1].start + pindices[-1].size
            self._write_quick_partition(
                start_pos,
                end,
                template,
                name,
                pindices,
                partition
            )

        return partition

    def _write_quick_partition(
        self,
        start: int,
        end: int,
        template: str,
        name: str,
        indices: Sequence[IndexRow],
        partition: int,
    ) -> None:
        size = (end - start)

        ffindex_name = template.format(
            name=name,
            index=partition,
            ext="ffindex"
        )

        ffdata_name = template.format(
            name=name,
            index=partition,
            ext="ffdata"
        )

        index_dirname = psplit(ffindex_name)[0]
        makedirs(index_dirname, exist_ok=True)

        data_dirname = psplit(ffdata_name)[0]
        makedirs(data_dirname, exist_ok=True)

        partition_index = FFIndex(indices).bump_starts(by=(-1 * start))

        with open(ffindex_name, "wb") as handle:
            partition_index.write_to(handle)

        with open(ffdata_name, "wb") as handle:
            self.data.write_sized(start, size, handle)

        return
