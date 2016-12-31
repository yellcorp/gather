import os


def recurse_file_iterator(roots):
    for path in roots:
        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            for container, _dirnames, filenames in os.walk(path):
                for filename in filenames:
                    yield os.path.join(container, filename)
