import argparse

from typing import Optional, List

from ffdb.exceptions import FFOrderError
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

    parser.add_argument(
        "ffdata",
        metavar="FFDATA_FILE",
        type=argparse.FileType('rb'),
        help="The ffindex .ffdata files.",
    )

    parser.add_argument(
        "ffindex",
        metavar="FFINDEX_FILE",
        type=argparse.FileType('rb'),
        help="The ffindex .ffindex files.",
    )

    return


def order(args: argparse.Namespace) -> None:
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

        if len(lorder) != len(db.index):
            raise FFOrderError((
                "If an order file is provided, it must have the "
                "same number of elements as the ffindex."
            ))

        torder: Optional[List[IndexRow]] = lorder

    else:
        torder = None

    outdb = FFDB.reorder_from(
        other=db,
        data_handle=args.data,
        order=torder
    )

    outdb.index.write_to(args.index)
    return
