from enum import Enum


class LinkResult(Enum):
    none = 0
    ok = 1
    unchanged = 2
    source_has_next = 3
    target_has_previous = 4


class Node(object):
    def __init__(self, element=None):
        self.element = element
        self.previous = None
        self.next = None

    def find_head(self):
        n = self
        while n.previous is not None:
            n = n.previous
        return n

    def find_tail(self):
        n = self
        while n.next is not None:
            n = n.next
        return n

    def chain(self):
        n = self.find_head()
        while n is not None:
            yield n
            n = n.next

    @staticmethod
    def link(a, b):
        if a is None or b is None:
            return LinkResult.none

        if a.next == b and b.previous == a:
            return LinkResult.unchanged

        if a.next is not None:
            return LinkResult.source_has_next

        if b.previous is not None:
            return LinkResult.target_has_previous

        a.next = b
        b.previous = a
        return LinkResult.ok


def extract_connected(node_list):
    extracted = set()

    for n in node_list:
        if n in extracted:
            continue
        head = n.find_head()
        extracted.update(head.chain())
        yield head
