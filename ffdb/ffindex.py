""" Classes for reading and writing ffindex databases. """

from collections import namedtuple
from copy import deepcopy
from shutil import copyfileobj
from io import BytesIO

IndexRow = namedtuple("IndexRow", ["name", "start", "size"])


class FFIndex(object):

    def __init__(self, index=None):
        """ Construct an ffindex given a list of index rows. """
        if index is None:
            index = []
        else:
            assert all(isinstance(r, IndexRow) for r in index)

        self.index = sorted(index, key=lambda x: x.start)
        self.lookup = dict()

        for i, idx in enumerate(self.index):
            # A well formed ffindex should never have duplicate names.
            assert idx.name not in self.lookup
            self.lookup[idx.name] = i

        return

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self.index[key]
        elif isinstance(key, (str, bytes)):
            return self.index[self.lookup[key]]
        else:
            raise ValueError("Expected either a string, an int, or a slice.")

    def __contains__(self, key):
        return key in self.lookup

    def __iter__(self):
        for row in self.index:
            yield row
        return

    def __len__(self):
        return len(self.index)

    @classmethod
    def from_file(cls, handle):
        indices = cls._parse_ffindex(handle)
        return cls(index=indices)

    @staticmethod
    def _parse_ffindex(handle):
        ffindex = list()
        for line in handle:
            name, start, size = line.strip().split()
            row = IndexRow(name, int(start), int(size))  # size - 1?
            ffindex.append(row)

        return ffindex

    def append(self, value):
        assert isinstance(value, IndexRow)

        name, start, size = value

        assert name not in self

        if len(self.index) > 0:
            last_row = self.index[-1]
            last_end = last_row.start + last_row.size
            start = last_end
        else:
            start = 0

        self.index.append(IndexRow(name, start, size))

        self.lookup[name] = len(self.index)
        return

    def extend(self, values):
        for value in values:
            self.append(value)
        return len(values)

    def write_to(self, handle):
        length = 0
        for ind in sorted(self.index, key=lambda x: x.name):
            line = "{}\t{}\t{}\n".format(
                ind.name.decode("utf-8"),
                ind.start,
                ind.size
            )
            length += handle.write(line.encode())

        return length

    def bump_starts(self, by=0):
        if by == 0:
            return deepcopy(self)

        new_index = []
        for name, start, size in self.index:
            new_index.append(IndexRow(name, start + by, size))

        # Construct a new object
        return self.__class__(new_index)


class FFData(object):

    def __init__(self, handle):
        self.handle = handle

    def __getitem__(self, key):
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

    def append(self, b):
        self.handle.seek(0, 2)  # Go to end of file.
        assert b[-1:] == b"\0"
        return self.handle.write(b)

    def write_to(self, handle):
        self.handle.seek(0)
        return copyfileobj(self.handle, handle)

    def write_sized(self, start, size, handle):
        self.handle.seek(start)
        return handle.write(self.handle.read(size))


class FFDB(object):

    def __init__(self, data, index):
        self.data = data
        self.index = index
        return

    @classmethod
    def from_file(cls, data_handle, index_handle):
        data = FFData(data_handle)
        index = FFIndex.from_file(index_handle)
        return cls(data, index)

    @classmethod
    def new(cls, data_handle=None):
        if data_handle is None:
            data_handle = BytesIO()

        data = FFData(data_handle)
        index = FFIndex()
        return cls(data, index)

    def __getitem__(self, key):
        indices = self.index[key]
        return self.data[indices]

    def __contains__(self, key):
        return key in self.index

    def __len__(self):
        return len(self.index)

    def append(self, data, key):
        if isinstance(key, IndexRow):
            this_key = key
        else:
            this_key = data.index[key]

        self.index.append(this_key)
        to_write = data.data[this_key]
        return self.data.append(to_write)

    def extend(self, data, keys):
        if isinstance(keys, slice):
            keys = data.index[keys]
        elif keys is None:
            keys = data.index

        length = 0
        for key in keys:
            length += self.append(data, key)
        return length

    def write_to(self, data_handle, index_handle):
        assert data_handle.tell() == 0

        l1 = self.index.write_to(index_handle)
        l2 = self.data.write_to(data_handle)
        return l1, l2

    def concat(self, dbs):
        # Go to end of file
        self.data.handle.seek(0, 2)
        for db in dbs:
            self.index.extend(db.index.index)
            db.data.write_to(self.data.handle)
        return

    def documents(self, trim=None):
        """ Iterate over all documents in a db into a single file.

        Optionally removing `trim` lines from the beginning of each
        document.
        """

        for index in self.index:

            # Take up to :-1 to strip the null byte
            document = self.data[index][:-1]

            if trim is not None:
                sdocument = document.split(b'\n')[trim:]
                if len(sdocument) == 0:
                    continue

                yield b'\n'.join(sdocument)
            else:
                yield document
        return

    def collect_into(self, outfile, trim=None):
        """ Collect all documents from a db into a single file.

        Optionally removing `trim` lines from the beginning of each
        document.
        """

        for document in self.documents(trim=trim):
            outfile.write(document)
            if not document.endswith(b'\n'):
                outfile.write(b'\n')
        return

    def partition(self, name, template="{name}_{index}.{ext}", n=10000):
        """ Chunk a database into partitions of size n """

        start_pos = 0
        pindices = []
        partition = 1

        for i, p in enumerate(self.index, 1):
            if i % n == 0:
                self._write_partition(
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
            self._write_partition(
                start_pos,
                end,
                template,
                name,
                pindices,
                partition
            )

        return partition

    def _write_partition(self, start, end, template, name, indices, partition):
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

        partition_index = FFIndex(indices).bump_starts(by=(-1 * start))

        with open(ffindex_name, "wb") as handle:
            partition_index.write_to(handle)

        with open(ffdata_name, "wb") as handle:
            self.data.write_sized(start, size, handle)

        return
