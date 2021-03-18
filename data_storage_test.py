from pathlib import Path

import tempfile
import time
import json

import subprocess

from IPython import embed

from data_storage import (
    DataGraph,
    NameSpaceGitWriter,
    DataNamespace,
    VERSION,
    DataObject,
)


class DBObject(DataObject):
    name: str


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
        n = DataNamespace(g, "data", nw)

        # Then.
        assert nw.repo.is_bare == True


def test_already_created_repository():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        subprocess.run(["git", "init", tmpdirname], check=True)

        # When.
        g = DataGraph()
        nw = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n = DataNamespace(g, "data", nw)

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


def test_repository_version_cmd_line():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g = DataGraph()
        nw = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n = DataNamespace(g, "data", nw)

        # When.
        version = json.loads(
            run_getoutput(
                ["git", "show", "refs/heads/master:metadata/version"], cwd=tmpdirname
            )
        )

        # Then.
        assert version == VERSION


def test_write_a_read_b():
    # Given.
    with tempfile.TemporaryDirectory() as tmpdirname:
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObject)

        a = DBObject(n_a, name="helloWorld")

        # When.
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(tmpdirname), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObject)

        b = DBObject(n_b, a.key)

        # Then.
        assert a.name == "helloWorld"
        assert b.name == "helloWorld"


# def test_sync_with_git_command_line():
#    with tempfile.TemporaryDirectory() as tmpdirname:
#        # Given.
#        dirA = Path(tmpdirname) / 'dirA'
#        dirB = Path(tmpdirname) / 'dirB'
#
#        dirA.mkdir()
#        dirB.mkdir()
#
#        g_a = DataGraph()
#        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
#        n_a = DataNamespace(g_a, "data", nw_a)
#        g_a.register_class(DBObject)
#
#        a = DBObject(n_a, name="helloWorld")
#
#        g_b = DataGraph()
#        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
#        n_b = DataNamespace(g_b, "data", nw_b)
#        g_b.register_class(DBObject)
#
#        # When.
#        run(['git', 'remote', 'add', 'origin', dirA], cwd=dirB)
#        run(['git', 'pull', 'origin', 'master'], cwd=dirB)
#
#        b = DBObject(n_b, a.key)
#
#        # Then.
#        assert a.name == "helloWorld"
#        assert b.name == "helloWorld"


def test_sync():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObject)

        a = DBObject(n_a, name="helloWorld")

        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObject)

        # When
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        nw_a.sync()
        nw_b.sync()
        # nw_a.sync()

        # import pdb; pdb.set_trace()

        ## When.
        # run(['git', 'remote', 'add', 'origin', dirA], cwd=dirB)
        # run(['git', 'pull', 'origin', 'master'], cwd=dirB)

        b = DBObject(n_b, a.key)

        # Then.
        assert a.name == "helloWorld"
        assert b.name == "helloWorld"


def test_sync_with_git_command_line_conflict():
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Given.
        dirA = Path(tmpdirname) / "dirA"
        dirB = Path(tmpdirname) / "dirB"

        dirA.mkdir()
        dirB.mkdir()

        # Add object in first repo with name "helloWorld"
        g_a = DataGraph()
        nw_a = NameSpaceGitWriter(Path(dirA), "refs/heads/master")
        n_a = DataNamespace(g_a, "data", nw_a)
        g_a.register_class(DBObject)

        a = DBObject(n_a, name="helloWorld")
        assert a.name == "helloWorld"

        # Add the same object/key in another object with name "fooBar".
        g_b = DataGraph()
        nw_b = NameSpaceGitWriter(Path(dirB), "refs/heads/master")
        n_b = DataNamespace(g_b, "data", nw_b)
        g_b.register_class(DBObject)

        b = DBObject(n_b, a.key, name="fooBar")
        assert b.name == "fooBar"

        # When

        # Link the repositories with remotes.
        nw_a.add_remote("origin", str(dirB))
        nw_b.add_remote("origin", str(dirA))

        # Sync the repositories. The repo A is supposed to have trhe value from repo B now.
        nw_a.sync()
        nw_b.sync()
        nw_a.sync()

        # Then.
        assert a.name == "fooBar"
        assert b.name == "fooBar"
