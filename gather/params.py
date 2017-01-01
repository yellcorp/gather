from enum import Enum
import collections


Config = collections.namedtuple(
    "Config", (
        "dir_template",
        "min_sequence_length",
        "ambiguity_behavior",
        "shared_directory_behavior",
        "rollback_behavior",
        "dry_run",
    )
)


class AmbiguityBehavior(Enum):
    ignore = 0
    report = 1
    cancel = 2

class SharedDirectoryBehavior(Enum):
    allow  = 0
    skip   = 1
    cancel = 2

class CancelReason(Enum):
    ambiguities = 1
    shared_directories = 2

class RollbackBehavior(Enum):
    set = 1
    all = 2

class GatherResult(Enum):
    ok = 0
    cancel = 2
    error_full_rollback = 3
    error_partial_rollback = 4
    error_failed_rollback = 5
