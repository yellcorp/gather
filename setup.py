#!/usr/bin/env python3


import gather

from setuptools import setup
import os
import sys


with open(os.path.join(
        os.path.dirname(sys.modules["__main__"].__file__),
        "readme.rst"
    ), "r") as readme_stream:
    readme_text = readme_stream.read()


setup(
    name             = "gather",
    version          = gather.__version__,
    description      = gather.__doc__.strip(),
    long_description = readme_text,
    author           = "Jim Boswell",
    author_email     = "jimb@yellcorp.org",
    license          = "MIT",
    url              = "https://github.com/yellcorp/gather",

    packages=[ "gather" ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Topic :: Utilities'
    ],
    entry_points={
        "console_scripts": [
            "gather = gather.cli:main",
        ]
    },
    # test_suite="tests"
)
