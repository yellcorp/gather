from argparse import ArgumentParser
import sys

from gather import (
    __version__,
    core,
    log,
    util,
)


DEFAULT_EPILOG = "The default is %(default)s."
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
        choices = enum_name_set(core.AmbiguityBehavior),
        default = core.AmbiguityBehavior.report.name,
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
        choices = enum_name_set(core.SharedDirectoryBehavior),
        default = core.SharedDirectoryBehavior.allow.name,
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
        choices = enum_name_set(core.RollbackBehavior),
        default = core.RollbackBehavior.all.name,
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
        "-v", "--verbose",
        action = "count",
        default = 0,
        help = """Increase logging level."""
    )

    p.add_argument(
        "-q", "--quiet",
        action = "count",
        default = 0,
        help = """Decrease logging level."""
    )

    p.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + __version__
    )

    return p


def main():
    sys.exit(run(sys.argv[1:]))


LOG_LEVELS = (
    log.ERROR,
    log.WARNING,
    log.INFO,
    log.VERBOSE,
    log.DEBUG,
)
def run(argv1=None):
    args = get_arg_parser().parse_args(argv1)

    paths = (
        util.recurse_file_iterator(args.paths)
        if args.recurse
        else args.paths
    )

    log_level = decide_log_level(LOG_LEVELS, log.INFO, args.verbose, args.quiet)

    logger = log.Logger(min_level=log_level)

    result = core.gather(
        paths = paths,
        dir_template = args.dir,
        min_sequence_length = args.min,
        ambiguity_behavior = core.AmbiguityBehavior[args.ambiguities],
        shared_directory_behavior = core.SharedDirectoryBehavior[args.shared],
        rollback_behavior = core.RollbackBehavior[args.rollback],
        dry_run = args.dry_run,
        logger = logger,
    )

    return result.value


def decide_log_level(selectable_levels, default_level, verbose, quiet):
    index = max(
        0,
        min(
            len(selectable_levels) - 1,
            selectable_levels.index(default_level) + verbose - quiet
        )
    )

    return selectable_levels[index]


def enum_name_set(enum_class):
    return frozenset(e.name for e in enum_class)
