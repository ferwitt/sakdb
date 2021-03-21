#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

__author__ = "Fernando Witt"
__credits__ = ["Fernando Witt"]

__license__ = "MIT"
__maintainer__ = "Fernando Witt"
__email__ = "ferawitt@gmail.com"

import shutil
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class SakDbExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        Extension.__init__(self, name, sources=[])
        self.sourcedir = Path(sourcedir).resolve()


class SakDbBuild(build_ext):
    def run(self) -> None:
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext: Extension) -> None:
        source_dir = ext.sourcedir / "sakdb"
        target_dir = Path(self.build_lib) / "sakdb"

        if target_dir.exists():
            shutil.rmtree(target_dir)
        shutil.copytree(source_dir, target_dir)

        # Create file to indicate module has type hinting.
        typed_file = target_dir / "py.typed"
        typed_file.touch()

        # Write the repo version in the installed package.
        init_file = target_dir / "__init__.py"
        with open(init_file, "a") as f:
            f.write("# SakDb\n")


setup(
    name="sakdb",
    version="0.1.0",
    ext_modules=[SakDbExtension("sakdb")],
    cmdclass=dict(build_ext=SakDbBuild),
    install_requires=["pygit2"],
)
