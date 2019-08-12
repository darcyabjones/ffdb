""" Function to run scripts specified in cli. """

import sys
from os.path import splitext
from os.path import basename

from ffdb.ffindex import FFDB
from ffdb.ffindex import IndexRow
from ffdb.seq import Seq
from ffdb.id_generator import IdConverter
from ffdb.cli import cli


def filter_duplicates(seqs, id_conv=None):
    # Seen maps checksums to ids
    seen = dict()

    for record in seqs:
        checksum = record.checksum()
        id_line = {"original_id": record.id, "checksum": checksum}

        if checksum in seen:
            id_line["id"] = seen[checksum]
            yield None, id_line
            continue

        if id_conv is None:
            id_line["id"] = record.id
        else:
            id_line["id"] = next(id_conv)

        record.id = id_line["id"]
        record.desc = None
        seen[checksum] = id_line["id"]
        yield record, id_line
    return


def filter_length(seqs, length):
    for seq in seqs:
        if len(seq) >= length:
            yield seq
    return


def dedup_fasta(seqs, out_mapping_handle):
    id_conv = IdConverter(prefix="FF", length=6)

    for record, id_line in filter_duplicates(seqs, id_conv=id_conv):
        out_mapping_handle.write(
            "{}\t{}\n".format(id_line["id"], id_line["old_id"])
        )
        if record is not None:
            yield record

    return


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

    seqs = Seq.parse_many(args.fasta)
    fseqs = filter_length(seqs, args.min_length)

    # Dedup_fasta writes to the mapping file directly
    if args.checksum is not None:
        iterator = dedup_fasta(fseqs, args.checksum)
    else:
        iterator = fseqs

    for record in iterator:
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

        chunk_data = bytearray()
        chunk_name = None
        chunk_size = 1

    if chunk_name is not None:
        chunk_data.extend(b'\0')
        index = IndexRow(chunk_name.encode(), 0, len(chunk_data))

        outdb.data.append(chunk_data)
        outdb.index.append(index)

    outdb.index.write_to(args.index)
    return


def collect(args):
    outfile = sys.stdout.buffer

    for (data, index) in zip(args.ffdata, args.ffindex):
        db = FFDB.from_file(data, index)
        db.collect_into(outfile, args.trim)
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
        else:
            raise ValueError("I shouldn't reach this point ever")
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
