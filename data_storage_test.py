import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from IPython import embed

from data_storage import (
    VERSION,
    DataGraph,
    DataNamespace,
    DataObject,
    NameSpaceGitWriter,
)


class DBObjectInt(DataObject):
    my_int: int


class DBObjectString(DataObject):
    my_string: str


class DBObjectDict(DataObject):
    my_dict: Dict[str, Any]


class DBObjectList(DataObject):
    my_list: List[str]


def run(cmd, cwd):
    subprocess.run(cmd, check=True, cwd=cwd)


def run_getoutput(cmd, cwd):
    p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, cwd=cwd)
    return p.stdout.strip().decode("utf-8")


def test_create_repository():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:

        # When.
        g = DataGraph()
        nw = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        DataNamespace(g, "data", nw)

        # Then.
        assert nw.repo.is_bare == True


def test_already_created_repository():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        subprocess.run(["git", "init", tmpdirname], check=True)

        # When.
        g = DataGraph()
        nw = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        DataNamespace(g, "data", nw)

        # Then.
        assert nw.repo.is_bare == False


def test_repository_version():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g = DataGraph()
        nw = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n = DataNamespace(g, "data", nw)

        # When.
        version = n.get_version()

        # Then.
        assert version == VERSION


def test_int_increment():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectInt)

        a = DBObjectInt(n_a, my_int=42)
        assert a.my_int == 42

        # When.
        a.my_int += 1

        # Then.
        assert a.my_int == 43


def test_write_a_read_b_int():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectInt)

        a = DBObjectInt(n_a, my_int=42)

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectInt)

        b = DBObjectInt(n_b, a.key)

        # Then.
        assert a.my_int == 42
        assert b.my_int == 42


def test_write_a_read_b_string():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_string="helloWorld")

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectString)

        b = DBObjectString(n_b, a.key)

        # Then.
        assert a.my_string == "helloWorld"
        assert b.my_string == "helloWorld"


def test_string_append():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_string="helloWorld")
        assert a.my_string == "helloWorld"

        # When.
        a.my_string += "!"

        # Then.
        assert a.my_string == "helloWorld!"


def test_wrwite_a_read_b_list():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectList)

        a = DBObjectList(n_a, my_list=[2, 3, 1, 5])

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
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


def DISABLED_test_list_append():
    # TODO(witt): Append is not working for lists.

    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectList)

        a = DBObjectList(n_a, my_list=[2, 3, 1, 5])
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert len(a.my_list) == 4

        # When.
        a.my_list.append(42)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert a.my_list[4] == 42
        assert len(a.my_list) == 5


def test_write_a_read_b_dict():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectDict)

        a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectDict)

        b = DBObjectDict(n_b, a.key)

        # Then.
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2


def DISABLED_test_list_pop_middle():
    # TODO(witt): The returned list will not trigger a change in the DB for appends nor for pop

    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectList)

        a = DBObjectList(n_a, my_list=[2, 3, 1, 5])
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 1
        assert a.my_list[3] == 5
        assert len(a.my_list) == 4

        # When.
        a.my_list.pop(2)

        # import pdb; pdb.set_trace()

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 3
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectList)

        b = DBObjectList(n_b, a.key)

        # Then.
        assert b.my_list[0] == 2
        assert b.my_list[1] == 3
        assert b.my_list[2] == 5
        assert len(b.my_list) == 3


def test_sync_string():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_string="helloWorld")

        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectString)

        # When
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        nw_a.sync()
        nw_b.sync()

        b = DBObjectString(n_b, a.key)

        # Then.
        assert a.my_string == "helloWorld"
        assert b.my_string == "helloWorld"


def test_sync_list():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectList)

        a = DBObjectList(n_a, my_list=[2, 1, 3])

        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectList)

        # When
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        nw_a.sync()
        nw_b.sync()

        b = DBObjectList(n_b, a.key)

        # Then.
        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3

        assert b.my_list[0] == 2
        assert b.my_list[1] == 1
        assert b.my_list[2] == 3


def test_sync_dict():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectDict)

        a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})

        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectDict)

        # When
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        nw_a.sync()
        nw_b.sync()

        b = DBObjectDict(n_b, a.key)

        # Then.
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2


def test_sync_with_git_command_no_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_string "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_string="helloWorld")
        assert a.my_string == "helloWorld"

        # Add the same object/key in another object with my_string "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectString)

        b = DBObjectString(n_b, a.key, my_string="fooBar")
        assert b.my_string == "fooBar"

        # When

        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()

        # Then.
        assert a.my_string == "fooBar"
        assert b.my_string == "fooBar"


def test_sync_list_with_git_command_no_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_list "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectList)

        a = DBObjectList(n_a, my_list=[2, 1, 3])
        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3
        assert len(a.my_list) == 3

        # Add the same object/key in another object with my_list "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectList)

        b = DBObjectList(n_b, a.key, my_list=[5, 4, 6, 10])
        assert b.my_list[0] == 5
        assert b.my_list[1] == 4
        assert b.my_list[2] == 6
        assert b.my_list[3] == 10
        assert len(b.my_list) == 4

        # When

        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

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


def test_sync_dict_with_git_command_no_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_dict "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectDict)

        a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        # Add the same object/key in another object with my_dict "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectDict)

        b = DBObjectDict(n_b, a.key, my_dict={"foo": 2, "hello": "world"})
        assert b.my_dict["foo"] == 2
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 2

        # When

        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert a.my_dict["foo"] == 2
        assert a.my_dict["bar"] == "hey"
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 3

        assert b.my_dict["foo"] == 2
        assert b.my_dict["bar"] == "hey"
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 3


def test_sync_with_git_command_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_string "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_string="helloWorld")
        assert a.my_string == "helloWorld"

        # Add the same object/key in another object with my_string "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectString)

        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        b = DBObjectString(n_b, a.key)
        assert b.my_string == "helloWorld"

        # When
        a.my_string = "changedA"
        b.my_string = "changedB"

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert a.my_string == "changedB"
        assert b.my_string == "changedB"


def test_sync_list_with_git_command_no_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_list "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectString)

        a = DBObjectString(n_a, my_list=[2, 1, 3])
        assert a.my_list[0] == 2
        assert a.my_list[1] == 1
        assert a.my_list[2] == 3
        assert len(a.my_list) == 3

        # Add the same object/key in another object with my_list "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectString)

        # When - Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        b = DBObjectList(n_b, a.key)
        assert b.my_list[0] == 2
        assert b.my_list[1] == 1
        assert b.my_list[2] == 3
        assert len(b.my_list) == 3

        # When.
        a.my_list = []
        b.my_list = [1, 2]

        # Then.
        assert len(a.my_list) == 0

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert len(b.my_list) == 2

        # When - Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert a.my_list[0] == 1
        assert a.my_list[1] == 2
        assert len(a.my_list) == 2

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert len(b.my_list) == 2

        # When.
        a.my_list = [3, 4, 5]
        b.my_list = [1, 2]

        # Then.
        assert a.my_list[0] == 3
        assert a.my_list[1] == 4
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert len(b.my_list) == 2

        # When - Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert a.my_list[0] == 1
        assert a.my_list[1] == 2
        assert a.my_list[2] == 5
        assert len(a.my_list) == 3

        assert b.my_list[0] == 1
        assert b.my_list[1] == 2
        assert b.my_list[2] == 5
        assert len(b.my_list) == 3


def test_sync_dict_with_git_command_no_common_base():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with my_dict "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObjectDict)

        a = DBObjectDict(n_a, my_dict={"foo": 1, "bar": "hey"})
        assert a.my_dict["foo"] == 1
        assert a.my_dict["bar"] == "hey"
        assert len(a.my_dict) == 2

        # Add the same object/key in another object with my_dict "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObjectDict)

        b = DBObjectDict(n_b, a.key)

        # When
        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert b.my_dict["foo"] == 1
        assert b.my_dict["bar"] == "hey"
        assert len(b.my_dict) == 2

        # When.
        a.my_dict = {"foo": 2, "hello": "world"}
        b.my_dict = {"foo": 3}

        nw_a.sync()
        nw_b.sync()
        nw_a.sync()
        nw_b.sync()

        # Then.
        assert a.my_dict["foo"] == 3
        assert a.my_dict["hello"] == "world"
        assert len(a.my_dict) == 2

        assert b.my_dict["foo"] == 3
        assert b.my_dict["hello"] == "world"
        assert len(b.my_dict) == 2
