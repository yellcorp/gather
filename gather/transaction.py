import os
import shutil


class TransactionError(OSError):
    pass


class RollbackError(Exception):
    def __init__(self, message, actions):
        super().__init__(message, actions)
        self.actions = actions


class Action(object):
    def __init__(self, *args):
        self._args = args

    def __repr__(self):
        return "%s(%s)" % (
            type(self).__name__,
            ", ".join(repr(a) for a in self._args)
        )

    __str__ = __repr__

    def execute(self):
        raise NotImplementedError()

    def undo_action(self):
        raise NotImplementedError()


class Move(Action):
    def __init__(self, src, dest):
        super().__init__(src, dest)
        self._src = src
        self._dest = dest

    def __str__(self):
        return "mv %s %s" % (self._src, self._dest)

    def execute(self):
        if not os.path.exists(self._dest):
            shutil.move(self._src, self._dest)
        else:
            raise TransactionError("Moving %s: Destination exists: %s" % (self._src, self._dest))

    def undo_action(self):
        return Move(self._dest, self._src)


class Mkdir(Action):
    def __init__(self, path):
        super().__init__(path)
        self._path = path

    def __str__(self):
        return "mkdir %s" % self._path

    def execute(self):
        os.mkdir(self._path)

    def undo_action(self):
        return Rmdir(self._path)


class Rmdir(Action):
    def __init__(self, path):
        super().__init__(path)
        self._path = path

    def __str__(self):
        return "rmdir %s" % self._path

    def execute(self):
        os.rmdir(self._path)

    def undo_action(self):
        return Mkdir(self._path)


class DryRunner(object):
    def rollback(self):
        raise NotImplementedError()

    def commit(self):
        pass

    def move(self, src, dest):
        pass

    def mkdirp(self, path):
        pass


class FilesystemTransaction(object):
    def __init__(self):
        self._undo = [ ]

    def commit(self):
        self._undo = [ ]

    def rollback(self):
        while len(self._undo) > 0:
            action = self._undo[-1]
            try:
                action.execute()
            except OSError as ose:
                raise RollbackError(
                    "Error rolling back",
                    self._undo[::-1]
                ) from ose

            self._undo.pop()

    def move(self, src, dest):
        self._execute_with_undo(Move(src, dest))

    def mkdirp(self, path):
        if path == "":
            return

        stack = [ ]

        while True:
            maker = Mkdir(path)

            try:
                self._execute_with_undo(maker)
                break

            except FileExistsError:
                if len(stack) == 0:
                    # this means it's the first path we tried to create,
                    # and it already exists, so there's no problem
                    break
                else:
                    # otherwise who knows. treat it like any other OSError
                    raise

            except FileNotFoundError:
                stack.append(maker)
                parent_path = os.path.dirname(path)
                if parent_path == path:
                    raise TransactionError("Reached a root while trying to create parent directories")
                elif parent_path == "":
                    break

        while len(stack) > 0:
            self._execute_with_undo(stack.pop())

    def _execute_with_undo(self, action):
        action.execute()
        self._push_undo(action)

    def _push_undo(self, action):
        self._undo.append(action.undo_action())
