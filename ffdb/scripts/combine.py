import argparse
from typing import List, Sequence, Dict
from typing import Tuple
from typing import BinaryIO

from ffdb.ffindex import FFDB, FFData, IndexRow
from ffdb.exceptions import FFOrderError


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
        "-s", "--sort",
        action="store_true",
        default=False,
        help=("Sort the combined databases by document "
              "length as you write them out.")
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


def parse_all_indices(
    index_files: Sequence[BinaryIO]
) -> List[Tuple[int, IndexRow]]:

    indices: List[Tuple[int, IndexRow]] = []

    for i, infile in enumerate(index_files):
        for line in infile:
            indices.append((i, IndexRow.parse_ffindex_line(line)))
    return indices


def parse_all_indices_as_dict(
    index_files: Sequence[BinaryIO]
) -> Dict[bytes, Tuple[int, IndexRow]]:

    indices: Dict[bytes, Tuple[int, IndexRow]] = {}

    for i, infile in enumerate(index_files):
        for line in infile:
            ir = IndexRow.parse_ffindex_line(line)
            indices[ir.name] = (i, ir)
    return indices


def parse_order(
    indices: Dict[bytes, Tuple[int, IndexRow]],
    handle: BinaryIO
) -> List[Tuple[int, IndexRow]]:

    order: List[Tuple[int, IndexRow]] = []

    for line in handle:
        sline = line.strip()
        if len(sline) == 0:
            continue

        ir = indices[sline]
        order.append(ir)

    if len(order) != len(indices):
        raise FFOrderError((
            "If an order file is provided, it must have the "
            "same number of elements as the ffindex."
        ))

    return order


def combine(args: argparse.Namespace):
    outdb = FFDB.new(args.data)

    if args.sort or args.order is not None:

        if args.order is not None:
            ind_dict = parse_all_indices_as_dict(args.ffindex)
            indices = parse_order(ind_dict, args.order)

        else:
            indices = parse_all_indices(args.ffindex)
            indices.sort(key=lambda x: x[1].size, reverse=True)

        data_map = [FFData(h) for h in args.ffdata]

        for i, ir in indices:
            chunk = data_map[i][ir]

            # We know that ir is always a single value, so this should be safe.
            assert isinstance(chunk, bytes)
            outdb.append(chunk, ir.name)

    else:
        indbs: List[FFDB] = []
        for (data, index) in zip(args.ffdata, args.ffindex):
            indb = FFDB.from_file(data, index)
            indbs.append(indb)

        # Writes to ffdata since new was provided handle.
        outdb.concat(indbs)

    outdb.index.write_to(args.index)
    return
