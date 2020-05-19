import argparse
import mmap

from typing import Optional, List, cast, BinaryIO

from ffdb.ffindex import FFDB, IndexRow


def cli_order(parser: argparse.ArgumentParser):
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
        "--order",
        type=argparse.FileType('rb'),
        default=None,
        help=(
            "When writing out the files, use this order instead of the "
            "default sorted order. Should be a file of newline separated ids, "
            "matching the first column of the ffindex file."
        )
    )

    # TODO add reverse option?

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
        help="The ffindex .ffdata file.",
    )

    parser.add_argument(
        "ffindex",
        metavar="FFINDEX_FILE",
        type=argparse.FileType('rb'),
        help="The ffindex .ffindex file.",
    )

    return


def order(args: argparse.Namespace) -> None:
    try:
        if args.mmap:
            mm: Optional[BinaryIO] = cast(
                BinaryIO,
                mmap.mmap(args.ffdata.fileno(), 0)
            )
            # For type checker
            assert mm is not None
            db = FFDB.from_file(mm, args.ffindex)
        else:
            mm = None
            db = FFDB.from_file(args.ffdata, args.ffindex)

        if args.order is not None:

            # This lorder junk is just for the typechecker
            lorder: List[IndexRow] = []

            for line in args.order:
                sline = line.strip()
                if len(sline) == 0:
                    continue

                ir = db.index[sline]
                assert isinstance(ir, IndexRow)
                lorder.append(ir)

            torder: Optional[List[IndexRow]] = lorder

        else:
            torder = None

        outdb = FFDB.reorder_from(
            other=db,
            data_handle=args.data,
            order=torder
        )

        outdb.index.write_to(args.index)

    finally:
        if mm is not None:
            mm.close()
    return
