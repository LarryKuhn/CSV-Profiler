#!/usr/bin/env python3

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="csvprofiler-larrykuhn", # Replace with your own username
    version="1.1.2",
    author="Larry Kuhn",
    author_email="LarryKuhn@outlook.com",
    description="An extensible CSV column profiling and validation utility",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/LarryKuhn/CSV-Profiler",
    packages=setuptools.find_packages(),
    scripts=["csvpcg.py", "csvprofiler.py", "wrapdemo.py"]
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Topic :: Office/Business",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing",
        "Topic :: Utilities"
    ],
    python_requires='>=3.6',
)