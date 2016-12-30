import os.path
import re
import collections

from gather import graph


__all__ = ("Collector", "Ambiguity")


Ambiguity = collections.namedtuple(
    "Ambiguity", (
        "direction",
        "file",
        "choices",
    )
)


NameInfo = collections.namedtuple(
    "NameInfo", (
        "path",
        "container",
        "name",
        "number",
        "value",
        "digit_count",
        "prefix",
        "suffix",
        "set_key",
    )
)

DIGIT_PATTERN = re.compile(r"(\d+)\D*$")
def extract_name_info(path):
    container, name = os.path.split(path)
    digit_match = DIGIT_PATTERN.search(name)

    if digit_match is None:
        return None

    number = digit_match.group(1)
    number_start, number_end = digit_match.span(1)

    prefix = name[:number_start]
    suffix = name[number_end:]

    return NameInfo(
        path = path,
        container = container,
        name = name,
        number = number,
        value = int(number),
        digit_count = len(number),
        prefix = prefix,
        suffix = suffix,
        set_key = (container, prefix, suffix),
    )


class SequenceInfo(object):
    def __init__(self, paths, first_info, last_info):
        self.paths = tuple(paths)
        self.first = first_info
        self.last = last_info

    def __str__(self):
        return os.path.join(
            self.container,
            "%s[%s-%s]%s" % (
                self.prefix,
                self.first.number,
                self.last.number,
                self.suffix
            )
        )

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            type(self).__name__,
            self.paths,
            self.first,
            self.last,
        )

    # the following properties are (should be) common to the set -
    # i.e. identical in both self.first and self.last
    @property
    def container(self):
        return self.first.container

    @property
    def prefix(self):
        return self.first.prefix

    @property
    def suffix(self):
        return self.first.suffix        


def lookup_key(name_info, digit_count_delta=0, value_delta=0):
    return name_info.set_key + (
        name_info.digit_count + digit_count_delta,
        name_info.value + value_delta
    )

class Collector(object):
    AMBIGUITY_PREVIOUS = -1
    AMBIGUITY_NEXT = 1

    def __init__(self):
        self._node_lookup = dict()
        self._all_nodes = [ ]
        self._ambiguous_nodes = set()
        self._ambiguities = [ ]

    def collect(self, path):
        name_info = extract_name_info(path)
        if name_info is None:
            return

        node = graph.Node(name_info)
        self._node_lookup[lookup_key(name_info)] = node
        self._all_nodes.append(node)
        self._insert(node)

    def collect_all(self, path_iter):
        for path in path_iter:
            self.collect(path)

    def has_ambiguities(self):
        return len(self._ambiguities) > 0

    def ambiguities(self):
        yield from self._ambiguities

    def sequences(self):
        for head in graph.extract_connected(self._all_nodes):
            sequence = self._node_chain_to_sequence(head)
            if sequence is not None:
                yield sequence

    def _node_chain_to_sequence(self, head):
        paths = [ ]

        node = head # pylint
        for node in head.chain():
            if node in self._ambiguous_nodes:
                return None
            paths.append(node.element.path)

        return SequenceInfo(paths, head.element, node.element)

    def _insert(self, node):
        name_info = node.element

        check_shorter = (
            name_info.value >= 10 and
            re.match(r"^10+$", name_info.number) is not None
        )

        check_longer = (
            not check_shorter and
            name_info.value >= 9 and
            re.match(r"^9+$", name_info.number) is not None
        )

        if name_info.value > 0:
            prev = self._get_neighbor(name_info, 0, -1)
            self._connect(prev, node)

            if check_shorter:
                prev = self._get_neighbor(name_info, -1, -1)
                self._connect(prev, node)

        if check_longer:
            next_ = self._get_neighbor(name_info, 1, 1)
        else:
            next_ = self._get_neighbor(name_info, 0, 1)
        self._connect(node, next_)

    def _get_neighbor(self, name_info, digit_count_delta, value_delta):
        key = lookup_key(name_info, digit_count_delta, value_delta)
        return self._node_lookup.get(key)

    def _connect(self, a, b):
        status = graph.Node.link(a, b)

        if status == graph.Node.LINK_SOURCE_HAS_NEXT:
            self._add_ambiguity(
                Collector.AMBIGUITY_NEXT,
                a, (a.next, b)
            )

        elif status == graph.Node.LINK_TARGET_HAS_PREVIOUS:
            self._add_ambiguity(
                Collector.AMBIGUITY_PREVIOUS,
                b, (b.previous, a)
            )

    def _add_ambiguity(self, direction, node, choices):
        self._ambiguous_nodes.add(node)
        self._ambiguous_nodes.update(choices)
        self._ambiguities.append(
            Ambiguity(
                direction,
                node.element.path,
                tuple(n.element.path for n in choices)
            )
        )
