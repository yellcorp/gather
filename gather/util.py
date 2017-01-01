import collections
import os


def enum_name_set(enum_class):
    return frozenset(e.name for e in enum_class)


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


def recurse_file_iterator(roots):
    for path in roots:
        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            for container, _dirnames, filenames in os.walk(path):
                for filename in filenames:
                    yield os.path.join(container, filename)


def filter_partition(func, iterable):
    trues = [ ]
    falses = [ ]

    for m in iterable:
        target = trues if func(m) else falses
        target.append(m)

    return trues, falses
