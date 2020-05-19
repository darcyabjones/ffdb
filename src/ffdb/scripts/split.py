import argparse
from os.path import basename, splitext
from mmap import mmap

from typing import Optional, List, BinaryIO, cast

from ffdb.exceptions import FFOrderError
from ffdb.ffindex import FFDB, IndexRow


def cli_split(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-n", "--size",
        type=int,
        default=100000,
        help="The number of records for each partition to have.",
    )

    parser.add_argument(
        "-b", "--basename",
        type=str,
        default="{name}_{index}.{ext}",
        help=(
            "The output database partition names. "
            "Can use python format syntax. "
            "Some values are available, `name` will be the basename of the "
            "input database (no extensions), `index` will be the 1-based "
            "partition number, and ext will be ffindex or ffdata as "
            "appropriate."
        )
    )

    parser.add_argument(
        "-u", "--unbalanced",
        action="store_true",
        default=False,
        help=(
            "Write out contiguous blocks rather than trying to take chunks "
            "from across the whole file. "
            "This is useful if all documents are about the same size and "
            "sorting is unnecessary."
        )
    )

    parser.add_argument(
        "--order",
        type=argparse.FileType('rb'),
        default=None,
        help=(
            "When writing out the files, use this order instead of the "
            "default sorted order. Should be a file of newline separated ids, "
            "matching the first column of the ffindex file."
        )
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


def simplename(path: str) -> str:
    return splitext(basename(path))[0]


def split(args: argparse.Namespace) -> None:
    try:
        if args.mmap:
            mm: Optional[BinaryIO] = cast(
                BinaryIO,
                mmap(args.ffdata.fileno(), 0)
            )
            # I know this looks stupid, it's for mypy
            assert mm is not None
            ffdb = FFDB.from_file(mm, args.ffindex)
        else:
            mm = None
            ffdb = FFDB.from_file(args.ffdata, args.ffindex)

        file_basename = simplename(args.ffdata.name)

        if args.unbalanced and args.order is None:
            ffdb.quick_partition(
                name=file_basename,
                template=args.basename,
                n=args.size,
            )

        else:

            if args.order is not None:

                # This lorder junk is just for the typechecker
                lorder: List[IndexRow] = []

                for line in args.order:
                    sline = line.strip()
                    if len(sline) == 0:
                        continue

                    ir = ffdb.index[sline]
                    assert isinstance(ir, IndexRow)
                    lorder.append(ir)

                order: Optional[List[IndexRow]] = lorder

            else:
                order = None

            if order is not None and len(order) != len(ffdb.index):
                raise FFOrderError((
                    "When providing and order file, there should be the "
                    "same number of items as in the ffindex file."
                ))

            ffdb.partition(
                name=file_basename,
                template=args.basename,
                n=args.size,
                order=order
            )
    finally:
        if mm is not None:
            mm.close()
    return
