import argparse
from typing import List, Sequence
from typing import Tuple
from typing import BinaryIO

from ffdb.ffindex import FFDB, FFData, IndexRow


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


def combine(args: argparse.Namespace):
    outdb = FFDB.new(args.data)

    if args.sort:
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
