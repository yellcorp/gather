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

    LINK_NONE = 0
    LINK_SOURCE_HAS_NEXT = -1
    LINK_TARGET_HAS_PREVIOUS = -2
    LINK_OK = 1
    @staticmethod
    def link(a, b):
        if a is None or b is None:
            return Node.LINK_NONE

        if a.next is not None:
            return Node.LINK_SOURCE_HAS_NEXT

        if b.previous is not None:
            return Node.LINK_TARGET_HAS_PREVIOUS

        a.next = b
        b.previous = a
        return Node.LINK_OK


def extract_connected(node_list):
    extracted = set()

    for n in node_list:
        if n in extracted:
            continue        
        head = n.find_head()
        yield head
        extracted.update(head.chain())
            