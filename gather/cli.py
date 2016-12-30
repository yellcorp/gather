from argparse import ArgumentParser
import sys

from gather import __version__
import gather.core as core


DEFAULT_EPILOG = "The default is %(default)s."

AMB_ENUM = dict(
    ignore = core.IGNORE,
    report = core.REPORT,
    cancel = core.CANCEL,
)

SHARE_ENUM = dict(
    allow  = core.ALLOW,
    skip   = core.SKIP,
    cancel = core.CANCEL,
)

ROLLBACK_ENUM = dict(
    set = core.ROLLBACK_SET,
    all = core.ROLLBACK_ALL,
)

def get_arg_parser():
    p = ArgumentParser(
        description = """Detect sets of files named with incrementing numbers,
        and move each set into its own new directory."""
    )

    p.add_argument(
        "paths",
        nargs = "+",
        metavar = "PATHS",
        help = """Files to gather."""
    )

    p.add_argument(
        "-r", "--recurse",
        action = "store_true",
        default = False,
        help = """If directories are specified on the command line, scan their
        contents for file sets as well."""
    )

    p.add_argument(
        "-d", "--dir",
        default = core.DEFAULT_DIR_TEMPLATE,
        metavar = "TEMPLATE",
        help = """Specify a template for naming new directories.  The template
        can contain literal text as well as the following tokens, which are
        substituted based on the detected file sequence.

        {path_prefix} - the shared prefix of all files in the set, including
        the path if any.

        {name_prefix} - the shared prefix of all files in the set, limited to
        the name only and not including any path.

        {suffix} - the shared suffix of all files in the set.

        {first} - the number of the first file in the set.

        {last} - the number of the last file in the set.

        {field} - a run of # characters, as many as their are digits in the
        number of the first file in the set. """ + DEFAULT_EPILOG
    )

    p.add_argument(
        "-m", "--min",
        type = int,
        default = 3,
        metavar = "COUNT",
        help = """Ignore sequences with fewer than %(metavar)s files. """ +
        DEFAULT_EPILOG
    )

    p.add_argument(
        "-a", "--ambiguities",
        choices = frozenset(AMB_ENUM.keys()),
        default = "report",
        metavar = "ACTION",
        help = """Specify handling of ambiguities. Ambiguities can occur if
        there are multiple files that could precede or follow a given file. In
        all cases, no action is taken on a sequence containing an ambiguity.
        `report` will list them. `ignore` will ignore them, unless --verbose is
        specified, in which case it is the same as `report`. `cancel` will exit
        without making any changes at all.  """ + DEFAULT_EPILOG
    )

    p.add_argument(
        "-s", "--shared",
        choices = frozenset(SHARE_ENUM.keys()),
        default = "allow",
        metavar = "ACTION",
        help = """Specify handling of shared directories. It is possible to
        specify a template for the --dir option that causes more than one
        sequence to be moved into a new directory. `allow` will permit multiple
        sequences to share a new directory. `skip` will skip any sequence that
        would share a new directory with another. `cancel` will exit without
        making any changes at all.  Note that even if `allow` is specified, it
        is considered an error if multiple files with identical names would be
        moved to the new directory.  """ + DEFAULT_EPILOG
    )

    p.add_argument(
        "--rollback",
        choices = frozenset(ROLLBACK_ENUM.keys()),
        default = "all",
        metavar = "ACTION",
        help = """Specify handling of errors. `all` will roll back every change
        made and exit. `set` will roll back only the changes to the set in
        which the error occurred and continue with the next.  """ +
        DEFAULT_EPILOG
    )

    p.add_argument(
        "-n", "--dry-run",
        action = "store_true",
        default = False,
        help = """List proposed changes without making them."""
    )

    p.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + __version__
    )

    return p


def main():
    return run(sys.argv[1:])


def run(argv1=None):
    args = get_arg_parser().parse_args(argv1)
    core.gather(
        paths = args.paths,
        recurse = args.recurse,
        dir_template = args.dir,
        min_sequence_length = args.min,
        ambiguities = AMB_ENUM[args.ambiguities],
        shared_directories = SHARE_ENUM[args.shared],
        rollback = ROLLBACK_ENUM[args.rollback],
        dry_run = args.dry_run,
    )
