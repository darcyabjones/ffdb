""" Function to run scripts specified in cli. """

import sys
from os.path import splitext
from os.path import basename

from ffdb.ffindex import FFDB
from ffdb.ffindex import IndexRow
from ffdb.seq import Seq

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

    if args.filter is not None:
        filter_out = {
            f.strip()
            for f
            in args.filter
        }

    seen = set()

    chunk_data = bytearray()
    chunk_name = None
    chunk_size = 1

    for record in Seq.parse_many(args.fasta):
        if args.checksum:
            checksum = record.checksum()

            if args.filter:
                if checksum in filter_out:
                    continue

            print("{}\t{}".format(record.id, checksum), file=args.mapping)

            if checksum in seen:
                continue
            else:
                record.id = checksum
                record.desc = None
                seen.add(checksum)

        elif args.filter:
            if record.id in filter_out:
                continue

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
        for index in db.index:

            # Take up to :-1 to strip the null byte
            document = db.data[index][:-1]

            if args.trim is not None:
                sdocument = document.split(b'\n')[args.trim:]
                if len(sdocument) == 0:
                    continue
                if sdocument[-1] != b'':
                    # Adds a newline when we join.
                    sdocument.append(b'')

                outfile.write(b'\n'.join(sdocument))
            else:
                outfile.write(document)
                if not document.endswith(b'\n'):
                    outfile.write(b'\n')
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
