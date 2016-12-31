import collections
import os

from gather.analyze import Collector
from gather.transaction import (
    DryRunner, FilesystemTransaction, RollbackError
)
import gather.log as log


def recurse_file_iterator(roots):
    for path in roots:
        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            for container, _dirnames, filenames in os.walk(path):
                for filename in filenames:
                    yield os.path.join(container, filename)


DEFAULT_DIR_TEMPLATE = "{path_prefix}[{first}-{last}]{suffix}"

IGNORE = 0
REPORT = 1
CANCEL = 2
ALLOW = 3
SKIP = 4

AMBIGUITY_CHOICES = (IGNORE, REPORT, CANCEL)
SHARED_DIRECTORY_CHOICES = (ALLOW, SKIP, CANCEL)

CANCEL_REASON_AMBIGUITIES = 1
CANCEL_REASON_SHARED = 2

ROLLBACK_SET = 1
ROLLBACK_ALL = 2

RESULT_OK = 0
RESULT_CANCEL = 2
RESULT_ERROR_FULL_ROLLBACK = 3
RESULT_ERROR_PARTIAL_ROLLBACK = 4
RESULT_ERROR_FAILED_ROLLBACK = 5

MSG_CANCEL_REASONS = (
    (CANCEL_REASON_AMBIGUITIES, "ambiguous sequences"),
    (CANCEL_REASON_SHARED,      "multiple sequences sharing a directory"),
)
MSG_CANCEL_REASONS_REPORT = "Stopping because {reasons}"

MSG_AMBIGUOUS_HEADER = "The following files are ambiguous sequence members:"
MSG_AMBIGUOUS_NEXT = "  {file} could be followed by {choices[0]} or {choices[1]}"
MSG_AMBIGUOUS_PREVIOUS = "  {choices[0]} or {choices[1]} could precede {file}"

MSG_SHARED_HEADER_ALLOWED = "Directory will contain multiple sequences:"
MSG_SHARED_HEADER_DISALLOWED = "Directory would contain multiple sequences:"
MSG_SHARED_COACH = "Use the --template option to create distinct directory names, or allow directories to contain multiple sequences with --share allow"

MSG_SHORT = "Skipping sequence {sequence_name} because its length ({sequence_length}) is below the minimum {min_length}"

MSG_DRY_RUN = "--dry-run specified. No changes will be made."

MSG_ERROR = "Error: {error!s}. Rolling back..."
MSG_ROLLBACK_OK = "Rollback complete"
MSG_ROLLBACK_FAIL = """\
Error while rolling back - incomplete commands follow.
# The first command listed failed with exception: {error!s}"""
MSG_ROLLBACK_FAIL_EPILOG = "# End incomplete commands"

MSG_ROLLBACK_COUNT = "{count} of {total} sequences failed and were rolled back."


def gather(
    paths,
    recurse = False,
    dir_template = DEFAULT_DIR_TEMPLATE,
    min_sequence_length = 1,
    ambiguity_behavior = REPORT,
    shared_directory_behavior = ALLOW,
    error_behavior = ROLLBACK_ALL,
    dry_run = False,
):
    logger = log.Logger()
    collector = Collector()

    path_iter = recurse_file_iterator(paths) if recurse else paths
    collector.collect_all(path_iter)

    plan, cancel_reasons = prepare(
        collector,
        sequence_name_generator(dir_template),
        logger,
        min_sequence_length,
        ambiguity_behavior,
        shared_directory_behavior,
    )

    if dry_run:
        logger.info(MSG_DRY_RUN)
        transactor = DryRunner()
    else:
        if len(cancel_reasons) > 0:
            return RESULT_CANCEL
        transactor = FilesystemTransaction()

    return execute_plan(plan, transactor, logger, error_behavior)


AMBIGUITY_BEHAVIOR_TO_LOG_LEVEL = {
    IGNORE: log.VERBOSE,
    REPORT: log.INFO,
    CANCEL: log.ERROR,
}
SHARED_DIRECTORY_BEHAVIOR_TO_LOG_LEVEL = {
    ALLOW:  log.VERBOSE,
    SKIP:   log.WARNING,
    CANCEL: log.ERROR,
}
def prepare(
    collector,
    sequence_namer,
    logger,
    min_sequence_length,
    ambiguity_behavior,
    shared_directory_behavior,
):
    plan = [ ]
    cancel_reasons = set()

    amb_log = logger.level_func(AMBIGUITY_BEHAVIOR_TO_LOG_LEVEL[ambiguity_behavior])
    share_log = logger.level_func(SHARED_DIRECTORY_BEHAVIOR_TO_LOG_LEVEL[shared_directory_behavior])

    if collector.has_ambiguities():
        if ambiguity_behavior == CANCEL:
            cancel_reasons.add(CANCEL_REASON_AMBIGUITIES)

        amb_log(MSG_AMBIGUOUS_HEADER)
        for amb in collector.ambiguities():
            amb_log(format_ambiguity(amb))
        amb_log("")

    sequences_by_parent = group(collector.sequences(), key=sequence_namer)
    found_shared = False
    for parent, sequences in sequences_by_parent:
        if len(sequences) > 1:
            share_log(
                MSG_SHARED_HEADER_ALLOWED
                if shared_directory_behavior == ALLOW
                else MSG_SHARED_HEADER_DISALLOWED
            )

            share_log("  %s" % parent)
            for sequence in sequences:
                share_log("    %s" % sequence)
            share_log("")

            if shared_directory_behavior != ALLOW:
                if not found_shared:
                    found_shared = True
                    if shared_directory_behavior == CANCEL:
                        cancel_reasons.add(CANCEL_REASON_SHARED)
                    logger.defer.info(MSG_SHARED_COACH)
                continue

        for sequence in sequences:
            if len(sequence.paths) < min_sequence_length:
                logger.info(
                    MSG_SHORT,
                    sequence_name = str(sequence),
                    sequence_length = len(sequence.paths),
                    min_length = min_sequence_length
                )
            else:
                plan.append((parent, sequence.paths))

    if len(cancel_reasons) > 0:
        logger.error(
            MSG_CANCEL_REASONS_REPORT,
            reasons = ", ".join(
                message for reason, message in MSG_CANCEL_REASONS
                if reason in cancel_reasons
            )
        )

    logger.defer.flush()

    return plan, cancel_reasons


def execute_plan(plan, transactor, logger, error_behavior):
    rollbacks = 0
    for parent, paths in plan:
        logger.info(parent)
        try:
            transactor.mkdirp(parent)
            for path in paths:
                logger.info("  %s" % path)
                new_path = os.path.join(parent, os.path.basename(path))
                transactor.move(path, new_path)
            if error_behavior == ROLLBACK_SET:
                transactor.commit()
            logger.info("")

        except OSError as ose:
            try:
                logger.error(MSG_ERROR, error=ose)
                transactor.rollback()
                logger.info(MSG_ROLLBACK_OK)
                if error_behavior == ROLLBACK_ALL:
                    return RESULT_ERROR_FULL_ROLLBACK
                else:
                    rollbacks += 1

            except RollbackError as re:
                logger.critical(MSG_ROLLBACK_FAIL, error=re)
                for action in re.actions:
                    logger.critical("  %s" % action)
                logger.critical(MSG_ROLLBACK_FAIL_EPILOG)
                raise re

    if rollbacks != 0:
        logger.warning(MSG_ROLLBACK_COUNT, count=rollbacks, total=len(plan))
        if rollbacks == len(plan):
            return RESULT_ERROR_FULL_ROLLBACK
        return RESULT_ERROR_PARTIAL_ROLLBACK
    return RESULT_OK


def sequence_name_generator(template):
    def generate(sequence):
        return template.format(
            path_prefix = os.path.join(sequence.container, sequence.prefix),
            name_prefix = sequence.prefix,
            suffix = sequence.suffix,
            first = sequence.first.number,
            last = sequence.last.number,
            field = "#" * sequence.first.digit_count,
        )
    return generate


def format_ambiguity(amb):
    template = (
        MSG_AMBIGUOUS_PREVIOUS
        if amb.direction == Collector.AMBIGUITY_PREVIOUS
        else MSG_AMBIGUOUS_NEXT
    )
    return template.format(**amb._as_dict())


def group(iterable, key=None):
    if key is None:
        key = lambda m: m

    result = collections.OrderedDict()

    for m in iterable:
        k = key(m)
        if k not in result:
            result[k] = [ m ]
        else:
            result[k].append(m)

    for k, ms in result.items():
        yield k, ms
