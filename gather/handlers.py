from gather.analyze import Direction
from gather.params import (
    AmbiguityBehavior,
    CancelReason,
    SharedDirectoryBehavior,
)
import gather.log as log


class Handler(object):
    def handle_ambiguities(self, amb_iter):
        pass

    def handle_rejected_sequences(self, sequences):
        pass

    def handle_shared_sequences(self, parent, dir_sequences):
        pass

    def handle_cancel_reasons(self, cancel_reasons):
        pass

    def plan_generation_complete(self):
        pass

    def before_sequence_move(self, target_dir):
        pass

    def before_file_move(self, old_path, new_path):
        pass

    def after_sequence_move(self, target_dir):
        pass

    def before_rollback(self, os_error):
        pass

    def after_rollback(self):
        pass

    def rollback_error(self, rollback_error):
        pass

    def plan_execution_complete(self, sequence_count, rollback_count):
        pass


class NoOpHandler(Handler):
    pass


MSG_AMBIGUOUS_HEADER = "The following files are ambiguous sequence members:"
MSG_AMBIGUOUS_NEXT = "  {file} could be followed by {choices[0]} or {choices[1]}"
MSG_AMBIGUOUS_PREVIOUS = "  {choices[0]} or {choices[1]} could precede {file}"

MSG_SHORT_HEADER = "The following sequences will be skipped because they are shorter than the minimum length {min_sequence_length}"

MSG_SHARED_HEADER_ALLOWED = "Directory will contain multiple sequences:"
MSG_SHARED_HEADER_DISALLOWED = "Directory would contain multiple sequences:"
MSG_SHARED_COACH = "Use the --template option to create distinct directory names, or allow directories to contain multiple sequences with --share allow"

MSG_CANCEL_REASONS = (
    (CancelReason.ambiguities,        "ambiguous sequences"),
    (CancelReason.shared_directories, "multiple sequences sharing a directory"),
)
MSG_CANCEL_REASONS_REPORT = "Stopping because {reasons}"

MSG_ERROR = "Error: {error!s}. Rolling back..."
MSG_ROLLBACK_OK = "Rollback complete"
MSG_ROLLBACK_FAIL = """\
Error while rolling back - incomplete commands follow.
# The first command listed failed with exception: {error!s}"""
MSG_ROLLBACK_FAIL_EPILOG = "# End incomplete commands"
MSG_ROLLBACK_COUNT = "{count} of {total} sequences failed and were rolled back."

MSG_DRY_RUN = "--dry-run specified. No changes were made."


AMBIGUITY_BEHAVIOR_TO_LOG_LEVEL = {
    AmbiguityBehavior.ignore: log.VERBOSE,
    AmbiguityBehavior.report: log.INFO,
    AmbiguityBehavior.cancel: log.ERROR,
}
SHARED_DIRECTORY_BEHAVIOR_TO_LOG_LEVEL = {
    SharedDirectoryBehavior.allow:  log.VERBOSE,
    SharedDirectoryBehavior.skip:   log.WARNING,
    SharedDirectoryBehavior.cancel: log.ERROR,
}
class CliReporter(Handler):
    def __init__(self, config, logger):
        self._config = config
        self._logger = logger

        self._log_amb = self._logger.level_func(
            AMBIGUITY_BEHAVIOR_TO_LOG_LEVEL[self._config.ambiguity_behavior]
        )

        self._log_share = self._logger.level_func(
            SHARED_DIRECTORY_BEHAVIOR_TO_LOG_LEVEL[self._config.shared_directory_behavior]
        )

        self._show_share_coach = False

    def handle_ambiguities(self, amb_iter):
        self._log_amb(MSG_AMBIGUOUS_HEADER)
        for amb in amb_iter:
            template = (
                MSG_AMBIGUOUS_PREVIOUS
                if amb.direction == Direction.previous
                else MSG_AMBIGUOUS_NEXT
            )
            self._log_amb(template, **amb._as_dict())

    def handle_rejected_sequences(self, sequences):
        header_level = (
            log.INFO if any(len(s.paths) > 1 for s in sequences)
            else log.VERBOSE
        )

        self._logger.log(
            header_level,
            MSG_SHORT_HEADER,
            min_sequence_length = self._config.min_sequence_length
        )

        for sequence in sequences:
            self._logger.log(
                log.INFO if len(sequence.paths) > 1 else log.VERBOSE,
                "  (%d) %s" % (len(sequence.paths), sequence)
            )

        self._logger.log(header_level, "")

    def handle_shared_sequences(self, parent, dir_sequences):
        allow_shared = self._config.shared_directory_behavior == SharedDirectoryBehavior.allow

        self._log_share(
            MSG_SHARED_HEADER_ALLOWED
            if allow_shared
            else MSG_SHARED_HEADER_DISALLOWED
        )

        self._log_share("  %s" % parent)
        for sequence in dir_sequences:
            self._log_share("    %s" % sequence)
        self._log_share("")

        if not allow_shared:
            self._show_share_coach = True

    def handle_cancel_reasons(self, cancel_reasons):
        reasons_text = ", ".join(
            message for reason, message in MSG_CANCEL_REASONS
            if reason in cancel_reasons
        )

        self._logger.error(MSG_CANCEL_REASONS_REPORT, reasons=reasons_text)

    def plan_generation_complete(self):
        if self._show_share_coach:
            self._logger.info(MSG_SHARED_COACH)

    def before_sequence_move(self, target_dir):
        self._logger.info(target_dir)

    def before_file_move(self, old_path, new_path):
        self._logger.info("  %s" % old_path)

    def after_sequence_move(self, target_dir):
        self._logger.info("")

    def before_rollback(self, os_error):
        self._logger.error(MSG_ERROR, error=os_error)

    def after_rollback(self):
        self._logger.info(MSG_ROLLBACK_OK)

    def rollback_error(self, rollback_error):
        self._logger.critical(MSG_ROLLBACK_FAIL, error=rollback_error)
        for action in rollback_error.actions:
            self._logger.critical("  %s" % action)
        self._logger.critical(MSG_ROLLBACK_FAIL_EPILOG)

    def plan_execution_complete(self, sequence_count, rollback_count):
        if rollback_count > 0:
            self._logger.warning(
                MSG_ROLLBACK_COUNT,
                count = rollback_count,
                total = sequence_count,
            )

        if self._config.dry_run:
            self._logger.info(MSG_DRY_RUN)
