import sys
import argparse

from ffdb.exceptions import FFError

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

    elif parsed.subparser_name is None:
        parser.print_help()
        sys.exit(1)

    return parsed


def main():
    args = cli(prog=sys.argv[0], args=sys.argv[1:])

    try:
        if args.subparser_name == "split":
            ffsplit(args)
        elif args.subparser_name == "combine":
            ffcombine(args)
        elif args.subparser_name == "fasta":
            from_fasta(args)
        elif args.subparser_name == "collect":
            collect(args)
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
