#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

__author__ = "Fernando Witt"
__credits__ = ["Fernando Witt"]

__license__ = "MIT"
__maintainer__ = "Fernando Witt"
__email__ = "ferawitt@gmail.com"

import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from sakdb.sakdb_storage import VERSION, SakDbGraph, SakDbNamespaceGit, SakDbObject


class DBObjectInt(SakDbObject):
    my_int: int


class DBObjectString(SakDbObject):
    my_string: str


class DBObjectDict(SakDbObject):
    my_dict: Dict[str, Any]


class DBObjectList(SakDbObject):
    my_list: List[int]


class DBObjectTwoFields(SakDbObject):
    my_bool: bool
    my_string: str


def run(cmd: List[str], cwd: str) -> None:
    subprocess.run(cmd, check=True, cwd=cwd)


def run_getoutput(cmd: List[str], cwd: str) -> str:
    p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, cwd=cwd)
    return p.stdout.strip().decode("utf-8")


def test_create_repository() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:

        # When.
        g = SakDbGraph()
        n = SakDbNamespaceGit(g, "data", Path(tmpdirname), "master")

        # Then.
        assert n.repo.is_bare is True


def test_already_created_repository() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        subprocess.run(["git", "init", tmpdirname], check=True)

        # When.
        g = SakDbGraph()
        n = SakDbNamespaceGit(g, "data", Path(tmpdirname), "master")

        # Then.
        assert n.repo.is_bare is False


def test_repository_version() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g = SakDbGraph()
        n = SakDbNamespaceGit(g, "data", Path(tmpdirname), "master")

        # When.
        version = n.get_version()

        # Then.
        assert version == VERSION


def test_repository_version_compatibility() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g = SakDbGraph()
        n = SakDbNamespaceGit(g, "data", Path(tmpdirname), "master")

        # When.
        ret1 = n._validate_version(
            ".".join([str(int(x) + 1) for x in VERSION.split(".")])
        )
        ret2 = n._validate_version(VERSION)
        ret3 = n._validate_version(
            ".".join([str(min(int(x) - 1, 0)) for x in VERSION.split(".")])
        )

        # Then.
        assert ret1 is False
        assert ret2 is True
        assert ret3 is True


def test_int_increment() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectInt)

        with g_a.session():
            a = DBObjectInt(n_a, my_int=42)

        assert a.my_int == 42

        # When.
        with g_a.session():
            a.my_int += 1

        # Then.
        assert a.my_int == 43


def test_rollback() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectInt)

        with g_a.session():
            # When.
            a = DBObjectInt(n_a, my_int=42)

            # Then.
            assert a.my_int == 42

        assert a.my_int == 42

        with g_a.session() as s:
            # When.
            a.my_int = 11

            # Then.
            assert a.my_int == 11

            # When.
            s.rollback()

            # Then.
            assert a.my_int == 42

        assert a.my_int == 42


def test_rollback_after_commit() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectInt)

        with g_a.session():
            # When.
            a = DBObjectInt(n_a, my_int=42)

            # Then.
            assert a.my_int == 42

        assert a.my_int == 42

        with g_a.session() as s:
            # When.
            a.my_int = 11
            s.commit()

            # Then.
            assert a.my_int == 11

            # When.
            s.rollback()

            # Then.
            assert a.my_int == 42

        assert a.my_int == 42


def test_rollback_exception_in_session() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectInt)

        with g_a.session():
            # When.
            a = DBObjectInt(n_a, my_int=42)

            # Then.
            assert a.my_int == 42

        assert a.my_int == 42

        with pytest.raises(Exception):
            with g_a.session() as s:
                # When.
                a.my_int = 11
                s.commit()

                # Then.
                assert a.my_int == 11

                # When.
                raise Exception("Something really bad happened.")

        # Then.
        assert a.my_int == 42


def test_two_fields() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectTwoFields)

        with g_a.session():
            # When.
            a = DBObjectTwoFields(n_a)
            a.my_int = 11
            a.my_string = "foo"

            # Then.
            assert a.my_int == 11
            assert a.my_string == "foo"

        assert a.my_int == 11
        assert a.my_string == "foo"


def test_write_a_read_b_int() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectInt)

        with g_a.session() as _:
            a = DBObjectInt(n_a, my_int=42)

        # When.
        g_b = SakDbGraph()
        g_b.register_class(DBObjectInt)

        with g_b.session() as _:
            n_b = SakDbNamespaceGit(g_b, "data", Path(tmpdirname), "master")
            b = DBObjectInt(n_b, a.key)

        # Then.
        assert a.my_int == 42
        assert b.my_int == 42


def test_write_a_read_b_string() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_string="helloWorld")

        # When.
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(tmpdirname), "master")

        g_b.register_class(DBObjectString)

        b = DBObjectString(n_b, a.key)

        # Then.
        assert a.my_string == "helloWorld"
        assert b.my_string == "helloWorld"


def test_string_append() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_string="helloWorld")

        assert a.my_string == "helloWorld"

        # When.
        with g_a.session():
            a.my_string += "!"

        # Then.
        assert a.my_string == "helloWorld!"


def test_wrwite_a_read_b_list() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectList)

        with g_a.session():
            a = DBObjectList(n_a, my_list=[2, 3, 1, 5])

        # When.
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(tmpdirname), "master")

        g_b.register_class(DBObjectList)

        b = DBObjectList(n_b, a.key)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert len(a.my_list) == 4

        assert b.my_list[0] == 2
        assert b.my_list[1] == 3
        assert b.my_list[2] == 1
        assert b.my_list[3] == 5
        assert len(b.my_list) == 4


def test_list_editions() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectList)

        with g_a.session():
            a = DBObjectList(n_a, my_list=[2, 3, 1, 5])

        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert len(a.my_list) == 4

        # When.
        with g_a.session():
            a.my_list.append(42)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert a.my_list[4] == 42
        assert len(a.my_list) == 5

        # When.
        with g_a.session():
            a.my_list += [100]

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert a.my_list[4] == 42
        assert a.my_list[5] == 100
        assert len(a.my_list) == 6

        # When.
        with g_a.session():
            a.my_list[5] = 101

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert a.my_list[4] == 42
        assert a.my_list[5] == 101
        assert len(a.my_list) == 6


def test_write_a_read_b_dict() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectDict)

        with g_a.session():
            a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        # When.
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(tmpdirname), "master")

        g_b.register_class(DBObjectDict)

        b = DBObjectDict(n_b, a.key)

        # Then.
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2


def test_dict_edit() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectDict)

        with g_a.session():
            a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        # When.
        with g_a.session():
            a.my_dict["hello"] = "world"

        # Then.
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 3

        # When.
        with g_a.session():
            a.my_dict.pop("foo")

        # Then.
        assert a.my_dict["bar"] == "hey"
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 2

        # When.
        with g_a.session():
            a.my_dict.update({"one_float": 1.23, "one_bool": True})

        # Then.
        assert a.my_dict["one_float"] == 1.23
        assert a.my_dict["one_bool"] is True
        assert a.my_dict["bar"] == "hey"
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 4


def test_list_pop_middle() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(tmpdirname), "master")

        g_a.register_class(DBObjectList)

        with g_a.session():
            a = DBObjectList(n_a, my_list=[2, 3, 1, 5])

        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert len(a.my_list) == 4

        # When.
        with g_a.session():
            a.my_list.pop(2)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        # When.
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(tmpdirname), "master")

        g_b.register_class(DBObjectList)

        b = DBObjectList(n_b, a.key)

        # Then.
        assert b.my_list[0] == 2
        assert b.my_list[1] == 3
        assert b.my_list[2] == 5
        assert len(b.my_list) == 3


def test_sync_string() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")

        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_string="helloWorld")

        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")

        g_b.register_class(DBObjectString)

        # When
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        n_a.sync()
        n_b.sync()

        b = DBObjectString(n_b, a.key)

        # Then.
        assert a.my_string == "helloWorld"
        assert b.my_string == "helloWorld"


def test_sync_list() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")

        g_a.register_class(DBObjectList)

        with g_a.session():
            a = DBObjectList(n_a, my_list=[2, 1, 3])

        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")

        g_b.register_class(DBObjectList)

        # When
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        n_a.sync()
        n_b.sync()

        b = DBObjectList(n_b, a.key)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3

        assert b.my_list[0] == 2
        assert b.my_list[1] == 1
        assert b.my_list[2] == 3


def test_sync_dict() -> None:
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")

        g_a.register_class(DBObjectDict)

        with g_a.session():
            a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectDict)

        # When
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        n_a.sync()
        n_b.sync()

        b = DBObjectDict(n_b, a.key)

        # Then.
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2


def test_sync_with_git_command_no_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_string "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_string="helloWorld")

        assert a.my_string == "helloWorld"

        # Add the same object/key in another object with my_string "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")

        g_b.register_class(DBObjectString)

        with g_b.session():
            b = DBObjectString(n_b, a.key, my_string="fooBar")

        assert b.my_string == "fooBar"

        # When

        # Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()

        # Then.
        assert a.my_string == "fooBar"
        assert b.my_string == "fooBar"


def test_sync_list_with_git_command_no_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_list "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectList)

        with g_a.session():
            a = DBObjectList(n_a, my_list=[2, 1, 3])

        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3
        assert len(a.my_list) == 3

        # Add the same object/key in another object with my_list "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectList)

        with g_b.session():
            b = DBObjectList(n_b, a.key, my_list=[5, 4, 6, 10])

        assert b.my_list[0] == 5
        assert b.my_list[1] == 4
        assert b.my_list[2] == 6
        assert b.my_list[3] == 10
        assert len(b.my_list) == 4

        # When

        # Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_list[0] == 5
        assert a.my_list[1] == 4
        assert a.my_list[2] == 6
        assert a.my_list[3] == 10
        assert len(a.my_list) == 4

        assert b.my_list[0] == 5
        assert b.my_list[1] == 4
        assert b.my_list[2] == 6
        assert b.my_list[3] == 10
        assert len(b.my_list) == 4


def test_sync_dict_with_git_command_no_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_dict "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectDict)

        with g_a.session():
            a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        # Add the same object/key in another object with my_dict "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectDict)

        with g_b.session():
            b = DBObjectDict(n_b, a.key, my_dict={"foo": 2, "hello": "world"})

        assert b.my_dict["foo"] == 2
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 2

        # When

        # Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_dict["foo"] == 2
        assert a.my_dict["bar"] == "hey"
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 3

        assert b.my_dict["foo"] == 2
        assert b.my_dict["bar"] == "hey"
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 3


def test_sync_with_git_command_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_string "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_string="helloWorld")

        assert a.my_string == "helloWorld"

        # Add the same object/key in another object with my_string "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectString)

        # Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        b = DBObjectString(n_b, a.key)
        assert b.my_string == "helloWorld"

        # When
        with g_a.session():
            a.my_string = "changedA"

        with g_b.session():
            b.my_string = "changedB"

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_string == "changedB"
        assert b.my_string == "changedB"


def test_sync_list_with_git_command_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_list "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectString)

        with g_a.session():
            a = DBObjectString(n_a, my_list=[2, 1, 3])

        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3
        assert len(a.my_list) == 3

        # Add the same object/key in another object with my_list "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectString)

        # When - Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        with g_b.session():
            b = DBObjectList(n_b, a.key)

        assert b.my_list[0] == 2
        assert b.my_list[1] == 1
        assert b.my_list[2] == 3
        assert len(b.my_list) == 3

        # When.
        with g_a.session():
            a.my_list = []

        with g_b.session():
            b.my_list = [1, 2]

        # Then.
        assert len(a.my_list) == 0

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert len(b.my_list) == 2

        # When - Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_list[0] == 1
        assert a.my_list[1] == 2
        assert len(a.my_list) == 2

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert len(b.my_list) == 2

        # When.
        with g_a.session():
            a.my_list = [3, 4, 5]

        with g_b.session():
            b.my_list = [11, 22]

        # Then.
        assert a.my_list[0] == 3
        assert a.my_list[1] == 4
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        assert b.my_list[0] == 11
        assert b.my_list[1] == 22
        assert len(b.my_list) == 2

        # When - Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_list[0] == 11
        assert a.my_list[1] == 22
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        assert b.my_list[0] == 11
        assert b.my_list[1] == 22
        assert b.my_list[2] == 5
        assert len(b.my_list) == 3


def test_sync_dict_with_git_command_common_base() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_dict "helloWorld"
        g_a = SakDbGraph()
        n_a = SakDbNamespaceGit(g_a, "data", Path(dirA), "master")
        g_a.register_class(DBObjectDict)

        with g_a.session():
            a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        # Add the same object/key in another object with my_dict "fooBar".
        g_b = SakDbGraph()
        n_b = SakDbNamespaceGit(g_b, "data", Path(dirB), "master")
        g_b.register_class(DBObjectDict)

        # When
        # Link the repositories with remotes.
        n_a.add_remote("origin", str(dirB))
        n_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        b = DBObjectDict(n_b, a.key)
        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2

        # When.
        with g_a.session():
            a.my_dict = {"foo": 2, "hello": "world"}
        with g_b.session():
            b.my_dict = {"foo": 3}

        n_a.sync()
        n_b.sync()
        n_a.sync()
        n_b.sync()

        # Then.
        assert a.my_dict["foo"] == 3
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 3
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 2
