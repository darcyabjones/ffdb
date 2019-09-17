import argparse

from ffdb.seq import Seq
from ffdb.ffindex import FFDB, IndexRow


def cli_fasta(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-d", "--data",
        required=True,
        type=argparse.FileType('wb'),
        help="The path to write the ffdata file to.",
    )

    parser.add_argument(
        "-i", "--index",
        required=True,
        type=argparse.FileType('wb'),
        help="The path to write the ffindex file to.",
    )

    parser.add_argument(
        "-n", "--size",
        type=int,
        default=1,
        help="The number of fasta records to use per document.",
    )

    parser.add_argument(
        "fasta",
        metavar="FASTA",
        nargs="+",
        type=argparse.FileType('rb'),
        help="The fasta files to pull in.",
    )

    return


def fasta(args: argparse.Namespace) -> None:
    outdb = FFDB.new(args.data)

    chunk_data = bytearray()
    chunk_name = None
    chunk_size = 1

    seqs = Seq.parse_many(args.fasta)

    for record in seqs:
        chunk_data.extend(bytes(record) + b'\n')

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
