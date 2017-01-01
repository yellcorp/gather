import os

from gather.analyze import Collector
from gather.handlers import NoOpHandler
from gather.params import (
    AmbiguityBehavior,
    CancelReason,
    GatherResult,
    RollbackBehavior,
    SharedDirectoryBehavior,
)
from gather.transaction import (
    DryRunner,
    FilesystemTransaction,
    RollbackError,
)
import gather.util as util


DEFAULT_DIR_TEMPLATE = "{path_prefix}[{first}-{last}]{suffix}"


def gather(
    paths,
    config,
    handler = None,
):
    if handler is None:
        handler = NoOpHandler()

    collector = Collector()

    collector.collect_all(paths)

    plan, cancel_reasons = prepare(
        collector,
        sequence_name_generator(config.dir_template),
        config.min_sequence_length,
        config.ambiguity_behavior,
        config.shared_directory_behavior,
        handler,
    )

    if config.dry_run:
        transactor = DryRunner()
    else:
        if len(cancel_reasons) > 0:
            return GatherResult.cancel
        transactor = FilesystemTransaction()

    return execute_plan(
        plan,
        transactor,
        config.rollback_behavior,
        handler,
    )


def prepare(
    collector,
    sequence_namer,
    min_sequence_length,
    ambiguity_behavior,
    shared_directory_behavior,
    handler,
):
    plan = [ ]
    cancel_reasons = set()

    if collector.has_ambiguities():
        handler.handle_ambiguities(collector.ambiguities())
        if ambiguity_behavior == AmbiguityBehavior.cancel:
            cancel_reasons.add(CancelReason.ambiguities)

    qualifying, rejected = util.filter_partition(
        lambda s: len(s.paths) >= min_sequence_length,
        collector.sequences()
    )

    if len(rejected) > 0:
        handler.handle_rejected_sequences(rejected)

    for parent, dir_sequences in util.group(qualifying, key=sequence_namer):
        if len(dir_sequences) > 1:
            handler.handle_shared_sequences(parent, dir_sequences)

            if shared_directory_behavior != SharedDirectoryBehavior.allow:
                if shared_directory_behavior == SharedDirectoryBehavior.cancel:
                    cancel_reasons.add(CancelReason.shared_directories)
                continue

        plan.extend((parent, sequence.paths) for sequence in dir_sequences)

    if len(cancel_reasons) > 0:
        handler.handle_cancel_reasons(cancel_reasons)

    handler.plan_generation_complete()

    return plan, cancel_reasons


def execute_plan(plan, transactor, error_behavior, handler):
    rollbacks = 0
    for parent, paths in plan:
        handler.before_sequence_move(parent)
        try:
            transactor.mkdirp(parent)
            for path in paths:
                new_path = os.path.join(parent, os.path.basename(path))
                handler.before_file_move(path, new_path)
                transactor.move(path, new_path)
            if error_behavior == RollbackBehavior.set:
                transactor.commit()
            handler.after_sequence_move(parent)

        except OSError as ose:
            try:
                handler.before_rollback(ose)
                transactor.rollback()
                handler.after_rollback()
                if error_behavior == RollbackBehavior.all:
                    return GatherResult.error_full_rollback
                else:
                    rollbacks += 1

            except RollbackError as re:
                handler.rollback_error(re)
                raise re

    handler.plan_execution_complete(len(plan), rollbacks)
    if rollbacks != 0:
        if rollbacks == len(plan):
            return GatherResult.error_full_rollback
        return GatherResult.error_partial_rollback
    return GatherResult.ok


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
