""" Argument parse for scripts. """

import sys
import argparse


def cli(prog, args):

    parser = argparse.ArgumentParser(
        prog=prog,
        description=""
    )

    subparsers = parser.add_subparsers(dest='subparser_name')

    split_subparser = subparsers.add_parser(
        "split",
        help="Split an ffindex database into n partitions."
    )

    # Add split parameters with sep function
    cli_split(split_subparser)

    combine_subparser = subparsers.add_parser(
        "combine",
        help="Collects many ffindex databases into a single one."
    )

    cli_combine(combine_subparser)

    fasta_subparser = subparsers.add_parser(
        "fasta",
        help=("Creates an ffindex database from a multifasta, "
              "with many sequences per document.")
    )

    cli_fasta(fasta_subparser)

    collect_subparser = subparsers.add_parser(
        "collect",
        help=("Collects all ffdata documents into a single file. "
              "Essentially it just filters out any null bytes and makes "
              "sure there is a newline between documents.")
    )

    cli_collect(collect_subparser)

    replace_ids_subparser = subparsers.add_parser(
        "replaceids",
        help="Replace ids in the file given a mapping file."
    )

    cli_replace_ids(replace_ids_subparser)

    parsed = parser.parse_args(args)

    # Validate arguments passed to combine
    if parsed.subparser_name in ("combine", "collect"):
        files = []
        files.extend(parsed.ffdata)
        files.extend(parsed.ffindex)

        if len(files) % 2 != 0:
            parser.error((
                "There should be the same number of ffindex and "
                "ffdata files provided to `combine`."
            ))

        parsed.ffdata = files[:len(files)//2]
        parsed.ffindex = files[len(files)//2:]

    return parsed


def cli_collect(parser):
    parser.add_argument(
        "-t", "--trim",
        type=int,
        default=None,
        help=("Trim this many lines from the start of each document. "
              "Useful for headers in csv documents."),
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


def cli_combine(parser):
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


def cli_fasta(parser):
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
        "-c", "--checksum",
        type=argparse.FileType('w'),
        default=None,
        help=(
            "Remove duplicates sequences and write mapping of new ids to "
            "original to file. Default: don't remove duplicates"
        ),
    )

    parser.add_argument(
        "-n", "--size",
        type=int,
        default=1,
        help="The number of fasta records to use per document.",
    )

    parser.add_argument(
        "-l", "--min-length",
        type=int,
        default=0,
        help="The minimum length of the sequence allowed. Default 0"
    )

    parser.add_argument(
        "-x", "--max-length",
        type=int,
        default=None,
        help="The maximum length of the sequence allowed. Default +inf"
    )

    parser.add_argument(
        "-s", "--strip",
        default=False,
        action="store_true",
        help="Strip stop codons (*) from end of sequences."
    )

    parser.add_argument(
        "-u", "--no-upper",
        default=False,
        action="store_true",
        help="Don't convert characters to uppercase."
    )

    parser.add_argument(
        "fasta",
        metavar="FASTA",
        nargs="+",
        type=argparse.FileType('r'),
        help="The fasta files to pull in.",
    )

    return


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


def cli_replace_ids(parser):
    parser.add_argument(
        "-m", "--map",
        type=argparse.FileType("r"),
        help="A file mapping ids to old ids.",
    )

    parser.add_argument(
        "-f", "--format",
        default="tsv",
        choices=["csv", "tsv", "fasta"],
        help="The format of the infile to parse.",
    )

    parser.add_argument(
        "-c", "--column",
        default=1,
        type=int,
        help=(
            "Specify the 1-based column to substitute with the map. "
            "If the file is a fasta, 1 is the id and 2 is the description. "
            "Default 1."
        )
    )

    parser.add_argument(
        "--header",
        default=False,
        action="store_true",
        help=(
          "The first line is treated as a header. "
          "Only respected for csv and tsv formats."
        )
    )

    parser.add_argument(
        "-o", "--outfile",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Write the output to a file rather than stdout.",
    )

    parser.add_argument(
        "infile",
        metavar="INFILE",
        type=argparse.FileType("r"),
        help="The file to replace ids in.",
    )

    return
