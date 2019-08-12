""" Function to run scripts specified in cli. """

import sys
from os.path import splitext
from os.path import basename

from ffdb.ffindex import FFDB
from ffdb.ffindex import IndexRow
from ffdb.seq import Seq
from ffdb.seq import Seqs
from ffdb.id_generator import IdConverter
from ffdb.cli import cli


def simplename(path):
    splitext(basename(path))[0]
    return


def ffsplit(args):
    ffdb = FFDB.from_file(args.ffdata, args.ffindex)

    file_basename = simplename(args.ffdata.name)
    ffdb.partition(
        name=file_basename,
        template=args.basename,
        n=args.size,
    )
    return


def ffcombine(args):
    outdb = FFDB.new(args.data)

    indbs = []
    for (data, index) in zip(args.ffdata, args.ffindex):
        indb = FFDB.from_file(data, index)
        indbs.append(indb)

    # Writes to ffdata since new was provided handle.
    outdb.concat(indbs)
    outdb.index.write_to(args.index)
    return


def from_fasta(args):
    outdb = FFDB.new(args.data)

    chunk_data = bytearray()
    chunk_name = None
    chunk_size = 1

    id_conv = IdConverter(prefix="FF", length=6)

    seqs = Seqs.parse_many(args.fasta)
    seqs = seqs.min_length(args.min_length)

    if args.max_length is not None:
        seqs = seqs.max_length(args.max_length)

    if args.strip:
        seqs = seqs.map(lambda x: x.rstrip("*"))

    if not args.no_upper:
        seqs = seqs.map(lambda x: x.upper())

    if args.checksum is not None:
        seqs = seqs.deduplicated(lambda x: next(id_conv))

    for record in seqs:
        chunk_data.extend(str(record).encode())

        # Handles first case after write, or just first case.
        if chunk_name is None:
            chunk_name = record.id

        if chunk_size % args.size != 0:
            chunk_size += 1
            continue

        chunk_data.extend(b'\0')
        index = IndexRow(chunk_name.encode(), 0, len(chunk_data))

        outdb.data.append(chunk_data)
        outdb.index.append(index)

        if args.checksum is not None:
            seqs.flush_ids(args.checksum)

        chunk_data = bytearray()
        chunk_name = None
        chunk_size = 1

    if chunk_name is not None:
        chunk_data.extend(b'\0')
        index = IndexRow(chunk_name.encode(), 0, len(chunk_data))

        outdb.data.append(chunk_data)
        outdb.index.append(index)

    if args.checksum is not None:
        seqs.flush_ids(args.checksum)

    outdb.index.write_to(args.index)
    return


def collect(args):
    outfile = sys.stdout.buffer

    for (data, index) in zip(args.ffdata, args.ffindex):
        db = FFDB.from_file(data, index)
        db.collect_into(outfile, args.trim)
    return


class ReplaceIdKeyError(Exception):

    def __init__(self, msg):
        self.msg = msg
        return


class InvalidOptionError(Exception):

    def __init__(self, msg):
        self.msg = msg
        return


def read_mapping(infile):
    d = dict()
    for line in infile:
        sline = line.strip().split("\t")
        d[sline[0]] = sline[1]
    return d


def get_id(id_map, id):
    if id in id_map:
        return id_map[id]
    else:
        raise ReplaceIdKeyError(f"Id {id} is not in the map file.")


def replace_ids(args):
    id_map = read_mapping(args.map)

    if args.format == "fasta":
        seqs = Seqs.parse(args.infile)

        if args.column == 1:
            seqs = map(
                lambda s: Seq(get_id(id_map, s.id), s.desc, s.seq),
                seqs
            )
        elif args.column == 2:
            seqs = map(
                lambda s: Seq(s.id, get_id(id_map, s.desc), s.seq),
                seqs
            )
        else:
            raise InvalidOptionError(
                f"Column {args.column} is not valid for fasta format.")

        for seq in seqs:
            args.outfile.write(str(seq))

    elif args.format in ("csv", "tsv"):
        if args.column < 1:
            raise InvalidOptionError("Column selection is 1-based.")

        if args.format == "csv":
            sep = ","
        else:
            sep = "\t"

        if args.header:
            header = next(args.infile)
            print(header, file=args.outfile)

        column = args.column - 1
        for line in args.infile:
            if line.startswith("#"):
                print(line, file=args.outfile)

            sline = line.strip().split(sep)
            try:
                sline[column] = get_id(id_map, sline[column])
            except IndexError:
                raise InvalidOptionError(
                    f"Encountered line with fewer than {args.column} columns.")

            print(sep.join(sline), file=args.outfile)
    else:
        print(args.format)
        raise ValueError("this shouldn't ever happen")
    return


def main():
    args = cli(prog=sys.argv[0], args=sys.argv[1:])

    try:
        if args.subparser_name == "split":
            ffsplit(args)
        elif args.subparser_name == "combine":
            ffcombine(args)
        elif args.subparser_name == "fasta":
            from_fasta(args)
        elif args.subparser_name == "collect":
            collect(args)
        elif args.subparser_name == "replaceids":
            replace_ids(args)
        else:
            raise ValueError("I shouldn't reach this point ever")
    except ReplaceIdKeyError as e:
        print(f"Error: {e.msg}")
        sys.exit(1)
    except InvalidOptionError as e:
        print(f"Error: {e.msg}")
        sys.exit(1)
    except EnvironmentError as e:
        print((
            "Encountered a system error.\n"
            "We can't control these, and they're usually related to your OS.\n"
            "Try running again."
        ), file=sys.stderr)
        raise e
    except KeyboardInterrupt:
        print("Received keyboard interrupt. Exiting.", file=sys.stderr)
        sys.exit(1)
    except BrokenPipeError:
        # Pipes get closed and that's normal
        sys.exit(0)
    except Exception as e:
        print((
            "I'm so sorry, but we've encountered an unexpected error.\n"
            "This shouldn't happen, so please file a bug report with the "
            "authors.\nWe will be extremely grateful!\n\n"
        ), file=sys.stderr)
        raise e

    return
