import collections
import os


def enum_name_set(enum_class):
    return frozenset(e.name for e in enum_class)


def group(iterable, key_function):
    """
    Groups values using a key function.

    Each item in iterable is passed as a single argument to `key`, and then
    added to a list associated with the return value. The groups is an
    iterator of (key_result, [ item, ... ]) tuples.

    :param iterable: An iterable.
    :param key_function: A function that accepts one argument and returns a
      hashable value, which will be used to group the items in iterable.
    """
    groups = collections.OrderedDict()

    for item in iterable:
        key = key_function(item)
        if key not in groups:
            groups[key] = [ item ]
        else:
            groups[key].append(item)

    for key, item_list in groups.items():
        yield key, item_list


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
