import argparse
import mmap

from typing import Set, Optional, cast, BinaryIO

from ffdb.ffindex import FFDB, IndexRow
from ffdb.exceptions import InvalidOptionError


def cli_select(parser: argparse.ArgumentParser):

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
        "--mmap",
        action="store_true",
        default=False,
        help=("Memory map the input hhdata file before reading chunks. "
              "This will significantly reduce IO overhead when doing balanced "
              "or sorted chunks, but requires enough memory to store the "
              "entire ffdata file."),
    )

    parser.add_argument(
        "-n", "--include",
        type=argparse.FileType('rb'),
        default=None,
        help=(
            "Only include ids from this file. Newline-delimited."
        )
    )

    parser.add_argument(
        "-e", "--exclude",
        type=argparse.FileType('rb'),
        default=None,
        help=(
            "Exclude any ids from this file. Newline-delimited."
        )
    )

    parser.add_argument(
        "ffdata",
        metavar="FFDATA_FILE",
        type=argparse.FileType('r+b'),
        help="The ffindex .ffdata files.",
    )

    parser.add_argument(
        "ffindex",
        metavar="FFINDEX_FILE",
        type=argparse.FileType('rb'),
        help="The ffindex .ffindex files.",
    )

    return


def select(args: argparse.Namespace) -> None:

    if args.include is None and args.exclude is None:
        raise InvalidOptionError(
            "Either --include or --exclude must be specified for "
            "'select' subcommand."
        )

    outdb = FFDB.new(args.data)

    include: Set[IndexRow] = set()
    if args.include is not None:
        for line in args.include:
            sline = line.strip()
            if len(sline) == 0:
                continue

            include.add(sline)

    exclude: Set[IndexRow] = set()
    if args.exclude is not None:
        for line in args.exclude:
            sline = line.strip()
            if len(sline) == 0:
                continue

            exclude.add(sline)

    try:
        if args.mmap:
            mm: Optional[BinaryIO] = cast(
                BinaryIO,
                mmap.mmap(args.ffdata.fileno(), 0)
            )
            # This is for typechecker
            assert mm is not None
            ffdb = FFDB.from_file(mm, args.ffindex)
        else:
            mm = None
            ffdb = FFDB.from_file(args.ffdata, args.ffindex)

        if len(include) > 0:
            included_rows = (ir for ir in ffdb.index if ir.name in include)
        else:
            included_rows = (ir for ir in ffdb.index)

        if len(exclude) > 0:
            irs = [
                ir
                for ir
                in included_rows
                if ir.name not in exclude
            ]
        else:
            irs = list(included_rows)

        irs.sort(key=lambda x: x.start)

        outdb.extend_from(ffdb, irs)
        outdb.index.write_to(args.index)

    finally:
        if mm is not None:
            mm.close()
    return
