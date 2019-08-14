import argparse
from os.path import basename, splitext

from ffdb.ffindex import FFDB


def cli_split(parser):
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


def simplename(path):
    splitext(basename(path))[0]
    return


def split(args):
    ffdb = FFDB.from_file(args.ffdata, args.ffindex)

    file_basename = simplename(args.ffdata.name)
    ffdb.partition(
        name=file_basename,
        template=args.basename,
        n=args.size,
    )
    return
