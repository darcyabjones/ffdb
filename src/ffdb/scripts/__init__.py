import sys
import argparse

from ffdb.exceptions import FFError
from ffdb.scripts.split import cli_split, split
from ffdb.scripts.combine import cli_combine, combine
from ffdb.scripts.collect import cli_collect, collect
from ffdb.scripts.fasta import cli_fasta, fasta
from ffdb.scripts.join_concat import cli_join_concat, join_concat
from ffdb.scripts.order import cli_order, order
from ffdb.scripts.select import cli_select, select


def cli(prog, args):

    parser = argparse.ArgumentParser(
        prog=prog,
        description="Scripts for manipulating ffindex databases."
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

    join_concat_subparser = subparsers.add_parser(
        "join_concat",
        help=("Collects all ffdata documents into a single file. "
              "Essentially it just filters out any null bytes and makes "
              "sure there is a newline between documents.")
    )

    cli_join_concat(join_concat_subparser)

    order_subparser = subparsers.add_parser(
        "order",
        help=("Sort an ffindex database by document size or by an external "
              "file of ids.")
    )

    cli_order(order_subparser)

    select_subparser = subparsers.add_parser(
        "select",
        help=("Select or filter documents from an ffindex database by name.")
    )

    cli_select(select_subparser)

    parsed = parser.parse_args(args)

    # Validate arguments passed to combine
    if parsed.subparser_name in ("combine", "collect", "join_concat"):
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

    elif parsed.subparser_name is None:
        parser.print_help()
        sys.exit(0)

    return parsed


def main():  # noqa
    args = cli(prog=sys.argv[0], args=sys.argv[1:])

    try:
        if args.subparser_name == "split":
            split(args)
        elif args.subparser_name == "combine":
            combine(args)
        elif args.subparser_name == "fasta":
            fasta(args)
        elif args.subparser_name == "collect":
            collect(args)
        elif args.subparser_name == "join_concat":
            join_concat(args)
        elif args.subparser_name == "order":
            order(args)
        elif args.subparser_name == "select":
            select(args)
        else:
            raise ValueError("I shouldn't reach this point ever")

    except FFError as e:
        print(f"Error: {e.msg}")
        sys.exit(e.ecode)

    except KeyboardInterrupt:
        print("Received keyboard interrupt. Exiting.", file=sys.stderr)
        sys.exit(130)

    except BrokenPipeError:
        # Pipes get closed and that's normal
        sys.exit(0)

    except EnvironmentError as e:
        print((
            "Encountered a system error.\n"
            "We can't control these, and they're usually related to your OS.\n"
            "Try running again."
        ), file=sys.stderr)
        raise e

    except Exception as e:
        print((
            "I'm so sorry, but we've encountered an unexpected error.\n"
            "This shouldn't happen, so please file a bug report with the "
            "authors.\nWe will be extremely grateful!\n\n"
        ), file=sys.stderr)
        raise e

    return
