import sys
import argparse
from ffdb.ffindex import FFDB


def cli_collect(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-t", "--trim",
        type=int,
        default=None,
        help=("Trim this many lines from the start of each document. "
              "Useful for headers in csv documents."),
    )

    parser.add_argument(
        "-o", "--outfile",
        type=argparse.FileType('wb'),
        default=sys.stdout.buffer,
        help=("Write to this file instead of stdout."),
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


def collect(args: argparse.Namespace) -> None:
    for (data, index) in zip(args.ffdata, args.ffindex):
        db = FFDB.from_file(data, index)
        db.collect_into(args.outfile, args.trim)
    return
