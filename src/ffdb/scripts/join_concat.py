import argparse
from collections import defaultdict

from ffdb.ffindex import FFDB
from ffdb.ffindex import IndexRow


def cli_join_concat(parser):
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


def join_concat(args):
    outdb = FFDB.new(args.data)

    indbs = []
    for (data, index) in zip(args.ffdata, args.ffindex):
        indb = FFDB.from_file(data, index)
        indbs.append(indb)

    index_names = defaultdict(list)
    for indb in indbs:
        for index_row in indb.index:
            index_names[index_row.name].append((index_row, indb.data))

    for index_name in index_names.keys():
        new_documents = []
        for index_row, data in index_names[index_name]:
            doc = data[index_row]
            new_documents.append(doc.rstrip(b"\0\n"))

        new_document = b'\n'.join(new_documents) + b'\n\0'
        new_index = IndexRow(index_name, 0, len(new_document))
        outdb.index.append(new_index)
        outdb.data.append(new_document)

    outdb.index.write_to(args.index)
    return
