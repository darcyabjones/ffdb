import argparse
from typing import List

from ffdb.ffindex import FFDB


def cli_combine(parser: argparse.ArgumentParser):
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
        "ffdata",
        metavar="FFDATA",
        nargs="+",
        type=argparse.FileType('rb'),
        help="The ffindex .ffdata file.",
    )

    parser.add_argument(
        "ffindex",
        metavar="FFINDEX",
        nargs="+",
        type=argparse.FileType('rb'),
        help="The ffindex .ffindex file.",
    )

    return


def combine(args: argparse.Namespace):
    outdb = FFDB.new(args.data)

    indbs: List[FFDB] = []
    for (data, index) in zip(args.ffdata, args.ffindex):
        indb = FFDB.from_file(data, index)
        indbs.append(indb)

    # Writes to ffdata since new was provided handle.
    outdb.concat(indbs)

    outdb.index.write_to(args.index)
    return
