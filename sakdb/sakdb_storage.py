import functools
import json
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TypeVar

import pygit2

from sakdb.sakdb_fields import SakDbField, SakDbFields, merge, sakdb_dumps, sakdb_loads

VERSION = "0.0.1"

_T = TypeVar("_T")
_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class SakDbSessionChanges(object):
    def __init__(self, namespace: "SakDbNamespace") -> None:
        super(SakDbSessionChanges, self).__init__()
        self.namespace = namespace

        # Dict containing paths and changes
        self.changes: Dict[str, "SakDbFields"] = {}

    def clear_changes(self) -> None:
        self.changes.clear()

    def read(self, path: Path) -> Optional["SakDbFields"]:
        if str(path) not in self.changes:
            return None
        return self.changes[str(path)]

    def write(self, path: Path, value: SakDbFields) -> None:
        previous_value = self.read(path)

        # Do not update timestamp if the content didn't change.
        if previous_value is not None:
            for prev_field in previous_value.fields:
                new_field = value.get_by_key(prev_field.key)

                if new_field is None:
                    continue

                if new_field.crc == prev_field.crc:
                    new_field.ts = prev_field.ts

        self.changes[str(path)] = merge(None, value, previous_value)

    def rollback(self) -> None:
        self.namespace.rollback()
        self.changes.clear()

    def commit(self, msg: str) -> None:
        self.namespace.commit(msg)
        self.changes.clear()

    def dump_to_namespace(self, msg: str) -> None:
        for path, value in self.changes.items():
            self.namespace.session_apply_sakdb(Path(path), value)
        self.changes.clear()

    def close_session(self, name: str, msg: str) -> None:
        self.namespace.close_session(name, msg)
        self.changes.clear()


class SakDbSession(object):
    def __init__(self, graph: "SakDbGraph", name: str, msg: str) -> None:
        super(SakDbSession, self).__init__()
        self.graph = graph

        # The idea is to have name as the session banch.
        self.name = name
        # msg is going to be used as the final commit message.
        self.default_msg = msg

        # Dict NamespaceName -> Changes.
        self.session_changes: Dict[str, SakDbSessionChanges] = {}

    def rollback(self) -> None:
        for changes in self.session_changes.values():
            changes.rollback()

        for changes in self.session_changes.values():
            changes.clear_changes()

    def commit(self, msg: Optional[str] = None) -> None:
        for changes in self.session_changes.values():
            message = msg or self.default_msg
            changes.dump_to_namespace(message)

        for changes in self.session_changes.values():
            changes.commit(msg or self.default_msg)

        for changes in self.session_changes.values():
            changes.clear_changes()

    def read_from_session(self, namespace: str, path: Path) -> Optional[SakDbFields]:
        if namespace not in self.session_changes:
            return None
        return self.session_changes[namespace].read(path)

    def write_to_session(
        self, namespace: "SakDbNamespace", path: Path, data: SakDbFields
    ) -> None:
        if namespace.name not in self.session_changes:
            namespace.start_session(self.name)
            self.session_changes[namespace.name] = SakDbSessionChanges(namespace)

        self.session_changes[namespace.name].write(path, data)

    def __enter__(self) -> "SakDbSession":
        # TODO(witt): Evaluate if it is necessary to use some sort of lock.
        return self

    def _perform_commit(self, namespace: "SakDbNamespace", msg: str) -> None:
        namespace.commit(msg)

    def __exit__(
        self,
        exception_type: type,
        exception_value: Exception,
        traceback: traceback.TracebackException,
    ) -> None:

        if exception_type or exception_value or traceback:
            self.rollback()
        else:
            for changes in self.session_changes.values():
                changes.dump_to_namespace(self.default_msg)

            for changes in self.session_changes.values():
                changes.close_session(self.name, self.default_msg)

        self.graph.current_session = None
        self.session_changes.clear()


class SakDbGraph(object):
    def __init__(self) -> None:
        super(SakDbGraph, self).__init__()

        self.namespaces: Dict[str, "SakDbNamespace"] = {}
        self.classes: Dict[str, type] = {}

        self.current_session: Optional[SakDbSession] = None

    def has_namespace_registered(self, name: str) -> bool:
        return name in self.namespaces

    def add_namepace(self, namespace: "SakDbNamespace") -> None:
        if not self.has_namespace_registered(namespace.name):
            self.namespaces[namespace.name] = namespace
        namespace.register_graph(self)

    def get_object(self, key: str) -> Optional["SakDbObject"]:
        for n in self.namespaces.values():
            obj = n.get_object(key)
            if obj is not None:
                return obj
        return None

    def register_class(self, cl: type) -> None:
        if cl.__name__ in self.classes:
            raise Exception(f"The class {cl.__name__} has been already registered")
        self.classes[cl.__name__] = cl

    def get_class(self, clname: str) -> type:
        return self.classes[clname]

    def get_objects(self) -> List["SakDbObject"]:
        ret = []
        for n in self.namespaces.values():
            ret += list(n.get_objects())
        return ret

    def session(self, name: str = "sakdbsession", msg: str = "Update") -> SakDbSession:
        if self.current_session is not None:
            raise Exception("There is already a session for the current graph.")

        self.current_session = SakDbSession(self, name, msg)
        return self.current_session


class SakDbNamespace(object):
    def __init__(self, graph: "SakDbGraph", name: str) -> None:
        super(SakDbNamespace, self).__init__()
        self.name = name

        self.objects: Dict[str, "SakDbObject"] = {}

        self.graph: Optional["SakDbGraph"] = graph
        self.register_graph(graph)

    def register_graph(self, graph: "SakDbGraph") -> None:
        self.graph = graph
        if not graph.has_namespace_registered(self.name):
            graph.add_namepace(self)

    def _validate_version(self, version: str) -> bool:
        # Extract the repository version components.
        repo_version, _, _ = (int(v) for v in version.split("."))

        # Extract the current software version components.
        software_version, _, _ = (int(v) for v in VERSION.split("."))

        # If the major version number is greater then the supported version.
        if repo_version > software_version:
            return False
        return True

    def node_keys(self) -> List[str]:
        return []

    def get_object_keys(self) -> Set[str]:
        # TODO: Use generators for this.
        return set(self.objects.keys()) | set(self.node_keys())

    def get_objects(self) -> List["SakDbObject"]:
        ret = []
        for key in self.get_object_keys():
            ret.append(self.get_object(key))
        return ret

    def get_object(self, key: str) -> "SakDbObject":
        # Check if there is already this key in the graph, then return it.
        # Otherwise create this object in the Namespace and return it.
        if key in self.objects:
            return self.objects[key]

        # Choose the proper class and then instantiate this specific class.
        cl_fields = self.read(key, "_cl")
        if cl_fields is None:
            raise Exception(f"Failed to read the class type for object {key}")

        clname_fields = cl_fields.get_by_key("_cl")
        if clname_fields is None:
            raise Exception(
                f"Failed to extract the class name from the DB for object {key}"
            )
        clname = clname_fields.payload

        if self.graph is None:
            raise Exception(f"No graph registered for namespace {self.name}!")
        else:
            cl = self.graph.get_class(clname)
            if cl is None:
                raise Exception(f"Class {clname} is not supported")
            else:
                obj = cl(self, key)

                if not isinstance(obj, SakDbObject):
                    raise Exception(
                        f"Object {obj} should be an instance of SakDbObject"
                    )

                self.register_object(obj)
                return obj

    def has_object(self, key: str) -> bool:
        try:
            obj = self.get_object(key)
        except Exception:
            return False
        return obj is not None

    def register_object(self, obj: "SakDbObject") -> None:
        if not self.has_object(obj.key):
            self.objects[obj.key] = obj

    def get_version(self) -> Optional[str]:
        ret = self.get_metadata("version")
        if not isinstance(ret, str):
            raise Exception(
                f"Version metadata was supposed to be string, however it is {type(ret)}."
            )
        return ret

    def read_sakdb(
        self, path: Path, branch: Optional[str] = None
    ) -> Optional[SakDbFields]:
        # Try to read from the Session, if not available read from the disk.
        if self.graph is not None:
            session = self.graph.current_session
            if session is not None:
                session_value = session.read_from_session(self.name, path)
                if session_value is not None:
                    return session_value

        value_str = self._read(path, branch)
        if value_str is None:
            return None
        return sakdb_loads(value_str)

    def read(self, node_key: str, data_key: str) -> Optional[SakDbFields]:
        node_path = (
            Path(self.name)
            / "objects"
            / node_key[0]
            / node_key[1]
            / node_key[2]
            / node_key[3]
            / node_key
        )
        data_path = node_path / data_key
        return self.read_sakdb(data_path)

    def get_metadata(self, key: str, branch: Optional[str] = None) -> Any:
        metada_path = Path(self.name) / "metadata" / key

        data = self.read_sakdb(metada_path)
        if data is None:
            raise Exception(f"Could not load the entry {key} from metadata DB")

        object_field = data.get_by_key(key)
        if object_field is None:
            raise Exception(f"No attribute {key} for {self}.")

        value = json.loads(object_field.payload)
        return value

    def write_sakdb(self, path: Path, value: SakDbFields) -> None:
        # Crashes if there is no session, otherwise write to it.
        if self.graph is None:
            raise Exception("It is necessary to have a graph to perform the write")

        session = self.graph.current_session
        if session is None:
            raise Exception(
                "It is necessary to be in a session scope to perform a write"
            )

        session.write_to_session(self, path, value)

    def session_apply_sakdb(self, path: Path, value: SakDbFields) -> None:
        # Check if content changed, if not do not update the timestamps.
        prev_value = self.read_sakdb(path)
        if prev_value is not None:
            for prev_field in prev_value.fields:
                new_field = value.get_by_key(prev_field.key)

                if new_field is None:
                    continue

                if new_field.crc == prev_field.crc:
                    new_field.ts = prev_field.ts

        # Dump sanitized timestamp value to the low level.
        value_str = sakdb_dumps(value)
        self._write(path, value_str)

    def write(self, node_key: str, data_key: str, value: SakDbFields) -> None:
        node_path = (
            Path(self.name)
            / "objects"
            / node_key[0]
            / node_key[1]
            / node_key[2]
            / node_key[3]
            / node_key
        )
        data_path = node_path / data_key
        self.write_sakdb(data_path, value)

    def set_metadata(self, key: str, value: Any) -> None:
        metada_path = Path(self.name) / "metadata" / key

        encoder = SakDbEncoder()
        payload_str = json.dumps(value, default=encoder.default, separators=(",", ":"))

        data = SakDbFields(
            SakDbField(key="_type", payload=type(value).__name__),
            SakDbField(key=key, payload=payload_str),
        )

        self.write_sakdb(metada_path, data)

    def rollback(self) -> None:
        raise Exception("Not implemented")

    def commit(self, msg: str) -> None:
        raise Exception("Not implemented")

    def start_session(self, name: str) -> None:
        raise Exception("Not implemented")

    def close_session(self, name: str, msg: str) -> None:
        raise Exception("Not implemented")

    def _read(self, path: Path, branch: Optional[str] = None) -> Optional[str]:
        raise Exception("Not implemented")

    def _write(self, path: Path, value: str) -> None:
        raise Exception("Not implemented")


class SakDbNamespaceGit(SakDbNamespace):
    def __init__(
        self, graph: "SakDbGraph", name: str, path: Path, branch: str = "master"
    ) -> None:
        super(SakDbNamespaceGit, self).__init__(graph, name)
        self.repo = pygit2.init_repository(path, True)

        self.namespace_branch = branch
        self.namespace_ref = f"refs/heads/{branch}"

        self._current_session_index: Optional[pygit2.Index] = None
        self._current_session_branch: Optional[str] = None

        if self.namespace_ref not in self.repo.references:
            # author = pygit2.Signature("a b", "a@b")
            author = self.repo.default_signature
            committer = author
            commit_message = "Initial commit"

            index = pygit2.Index()
            tid = index.write_tree(self.repo)

            self.repo.create_commit(
                self.namespace_ref, author, committer, commit_message, tid, []
            )

        # Read the version and check if it is compatible with the current implementation.
        try:
            version = self.get_metadata("version")
        except Exception:
            version = None

        if version is not None:
            if not self._validate_version(version):
                raise Exception(
                    f"Repo version ({version}) not supported, please update the system."
                )
        else:
            # If no version was found, just store the current version.
            if self.graph is None:
                raise Exception("No graph configured")
            with self.graph.session(msg="Set version"):
                self.set_metadata("version", VERSION)

    def add_remote(self, name: str, url: str) -> pygit2.remote.Remote:
        # TODO(witt): What if remote already exists?
        return self.repo.remotes.create(name, url)

    def do_merge(
        self, synced_branch: pygit2.IndexEntry, theirs_branch: pygit2.IndexEntry
    ) -> None:
        theirs_commit = self.repo[theirs_branch.target]

        # synced_branch = self.repo.branches[synced_branch.name]
        merge_result, _ = self.repo.merge_analysis(
            theirs_branch.target, synced_branch.name
        )

        if merge_result & pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE:
            return

        merged_index = self.repo.merge_commits(synced_branch, theirs_commit)

        if merged_index.conflicts:

            for (base_index, ours_index, theirs_index) in list(merged_index.conflicts):
                path = None

                base = None
                if base_index is not None:
                    base = sakdb_loads(self.repo[base_index.oid].data.decode("utf-8"))
                    path = Path(base_index.path)
                ours = None
                if ours_index is not None:
                    ours = sakdb_loads(self.repo[ours_index.oid].data.decode("utf-8"))
                    path = Path(ours_index.path)
                theirs = None
                if theirs_index is not None:
                    theirs = sakdb_loads(
                        self.repo[theirs_index.oid].data.decode("utf-8")
                    )
                    path = Path(theirs_index.path)

                merged = merge(base, ours, theirs)
                if merged is None:
                    raise Exception("There was some unresolved merge conflict here.")

                merged_str = sakdb_dumps(merged)

                blob = self.repo.create_blob(merged_str)
                new_entry = pygit2.IndexEntry(str(path), blob, pygit2.GIT_FILEMODE_BLOB)
                merged_index.add(new_entry)

                del merged_index.conflicts[str(path)]

        user = self.repo.default_signature
        tree = merged_index.write_tree()
        message = f"Merge {theirs_branch.name}"

        # Move the namespace branch to the synced branch.
        self.repo.create_commit(
            synced_branch.name,
            user,
            user,
            message,
            tree,
            [synced_branch.target, theirs_branch.target],
        )

    def sync(self, remotes: List[str] = []) -> None:
        for remote in self.repo.remotes:
            remote.fetch()

        for branch_name in self.repo.branches.local:
            branch = self.repo.branches[branch_name]

            if branch_name.startswith("synced/"):
                continue

            # I will create the sync branch.
            synced_branch_name = f"synced/{branch_name}"
            if synced_branch_name not in self.repo.branches:
                branch_commit = self.repo[branch.target]
                self.repo.branches.local.create(synced_branch_name, branch_commit)

            # Merge the synced branch with the local branch.
            synced_branch = self.repo.branches[synced_branch_name]
            self.do_merge(synced_branch, branch)

            # Merge the synced branch with all the remote synced branches.
            for remote_branch_name in self.repo.branches.remote:

                if not remote_branch_name.endswith(synced_branch_name):
                    continue
                remote_branch = self.repo.branches[remote_branch_name]

                remote_branch_version = self.get_metadata(
                    "version", remote_branch.target
                )
                if remote_branch_version is not None:
                    if not self._validate_version(remote_branch_version):
                        msg = (
                            f"WARNING! Cannot sync with {remote_branch_name} because the version"
                            f" {remote_branch_version} is not compatible with {VERSION}"
                        )
                        raise Exception(msg)
                        # continue

                synced_branch = self.repo.branches[synced_branch_name]
                self.do_merge(synced_branch, remote_branch)

            # Move the local branch to the synced branch.
            synced_branch = self.repo.branches[synced_branch_name]
            branch.set_target(synced_branch.target)

            for remote in self.repo.remotes:
                remote.push([synced_branch.name])
                remote.fetch()

    def node_keys(self) -> List[str]:
        # TODO: This should be a generator.
        ret = []

        namespace_ref = self.repo.references[self.namespace_ref].target
        tree = self.repo[namespace_ref].tree / self.name / ["objects"]
        for obj in tree:
            if obj.type_str == "tree":
                for obj2 in self.repo[obj.id]:
                    ret.append(obj2.name)

        return ret

    def _read_blob(self, ref: str, path: Path) -> Optional[pygit2.Object]:
        try:
            path_parts = path.parts

            branch_ref = self.repo.references[ref].target
            tree = self.repo[branch_ref].tree

            current_node = tree
            for ipath in path_parts:
                current_node = current_node[ipath]

            return current_node
        except Exception:
            # Exception("Could not read {path} in {self}.")
            return None

    def _read(self, path: Path, branch: Optional[str] = None) -> Optional[str]:
        blob: Optional[pygit2.Object] = None

        if branch is not None:
            # Try in session index.
            blob = self._read_blob(branch, path)
        elif self._current_session_branch is not None:
            # Try in session index.
            blob = self._read_blob(f"refs/heads/{self._current_session_branch}", path)
        else:
            # Try in namespace branch.
            blob = self._read_blob(f"refs/heads/{self.namespace_branch}", path)

        if blob is not None:
            ret = blob.data.decode("utf-8")
            if isinstance(ret, str):
                return ret
            else:
                return None
        else:
            # Exception("Could not read {path} in {self}.")
            return None

    def _write(self, path: Path, value: str) -> None:

        blob = self.repo.create_blob(value)

        # Check if the blob is identical, then just return.
        previous_blob: Optional[pygit2.Object] = None
        if self._current_session_branch is not None:
            # Try in session index.
            previous_blob = self._read_blob(
                f"refs/heads/{self._current_session_branch}", path
            )
        else:
            # Try in namespace branch.
            previous_blob = self._read_blob(f"refs/heads/{self.namespace_branch}", path)

        if previous_blob is not None:
            if blob == previous_blob.oid:
                return

        if self._current_session_index is None:
            raise Exception("There should be a index setup")

        new_entry = pygit2.IndexEntry(str(path), blob, pygit2.GIT_FILEMODE_BLOB)

        self._current_session_index.add(new_entry)

    def start_session(self, name: str) -> None:

        if self._current_session_index is not None:
            raise Exception("Index was supposed to be None")
        if self._current_session_branch is not None:
            raise Exception("Session was supposed to be None")

        session_branch = f"session/{name}"
        # If the branch already exists, just append a unique identifier.
        if session_branch in self.repo.branches:
            session_branch = session_branch + "." + uuid.uuid4().hex[:7]

        # Create the session branch.
        self.repo.create_branch(
            session_branch, self.repo.references[self.namespace_ref].peel()
        )

        # Store the session branch and index.
        self._current_session_branch = session_branch

        self._current_session_index = pygit2.Index()

        # Populate the index with the content of the namespace branch.
        namespace_ref = self.repo.references[self.namespace_ref].target
        tree = self.repo[namespace_ref].tree
        self._current_session_index.read_tree(tree)

    def close_session(self, name: str, msg: str) -> None:
        if self._current_session_branch is None:
            raise Exception("there is not active session")
        if self._current_session_index is None:
            raise Exception("there is not active session")

        # Do a final commit.
        self.commit(msg)

        # Move the namespace branch to the session branch.
        branch = self.repo.branches[self.namespace_branch]
        session_branch = self.repo.branches[self._current_session_branch]

        self.do_merge(branch, session_branch)

        self.repo.branches.delete(self._current_session_branch)

        self._current_session_index = None
        self._current_session_branch = None

    def rollback(self) -> None:
        # Reset the session banch to the namespace branch.

        if self._current_session_branch is None:
            raise Exception("Something went wrong. The session branch is not set")

        # Move the session branch back to the namespace.
        branch = self.repo.branches[self.namespace_branch]
        session_branch = self.repo.branches[self._current_session_branch]
        session_branch.set_target(branch.target)

        # Reset the index.
        namespace_ref = self.repo.references[self.namespace_ref].target
        tree = self.repo[namespace_ref].tree

        self._current_session_index = pygit2.Index()
        self._current_session_index.read_tree(tree)

    def commit(self, msg: str) -> None:

        if self._current_session_index is None:
            raise Exception(
                "Cannot commit to the current index, it is None. Make sure you called start session"
            )
        if self._current_session_branch is None:
            raise Exception("Something went wrong. The session branch is not set")

        # Write the index into a tree.
        tid = self._current_session_index.write_tree(self.repo)

        # The session branch.
        session_ref_str = f"refs/heads/{self._current_session_branch}"
        session_ref = self.repo.references[session_ref_str].target
        tree = self.repo[session_ref].tree

        # If the tree id is different, then commit it.
        if tid != tree.id:
            # author = pygit2.Signature("a b", "a@b")
            author = self.repo.default_signature
            committer = author
            commit_message = msg

            self.repo.create_commit(
                session_ref_str, author, committer, commit_message, tid, [session_ref]
            )


class SakDbEncoder(json.JSONEncoder):
    def __init__(self) -> None:
        super(SakDbEncoder, self).__init__()

    def default(self, value: Any) -> Any:
        if isinstance(value, SakDbObject):
            return {
                "_type": "SakDbObject",
                "nm": value.namespace.name,
                "key": value.key,
            }
        else:
            return super(SakDbEncoder, self).default(value)


class SakDbDecoder(object):
    def __init__(self, graph: "SakDbGraph") -> None:
        super(SakDbDecoder, self).__init__()
        self.graph = graph

    def object_hook(self, value: Dict[str, Any]) -> Any:
        if "_type" in value:
            if value["_type"] == "SakDbObject":
                return self.graph.get_object(value["key"])
        return value


class SakDbList(list):  # type: ignore
    def __init__(
        self, obj: "SakDbObject", obj_name: str, *args: Any, **vargs: Any
    ) -> None:
        super(SakDbList, self).__init__(*args, **vargs)
        self._obj = obj
        self._obj_name = obj_name

    def __getattribute__(self, key: str) -> Any:
        if key in [
            "__setitem__",
            "append",
            "extend",
            "insert",
            "remove",
            "pop",
            "clear",
            "sort",
            "reverse",
        ]:
            func = super(SakDbList, self).__getattribute__(key)

            @functools.wraps(func)
            def wrapper(*args: Any, **vargs: Any) -> Any:
                ret = func(*args, **vargs)
                setattr(self._obj, self._obj_name, self)
                return ret

            return wrapper
        return super(SakDbList, self).__getattribute__(key)

    def __setitem__(self, key: int, item: Any) -> None:  # type: ignore
        super(SakDbList, self).__setitem__(key, item)
        setattr(self._obj, self._obj_name, self)


class SakDbDict(Dict[_KT, _VT]):
    def __init__(self, obj: "SakDbObject", obj_name: str, *args, **vargs) -> None:  # type: ignore
        super(SakDbDict, self).__init__(*args, **vargs)
        self._obj = obj
        self._obj_name = obj_name

    def __getattribute__(self, key: str) -> Any:
        if key in ["clear", "update", "pop"]:
            func = super(SakDbDict, self).__getattribute__(key)

            @functools.wraps(func)
            def wrapper(*args: Any, **vargs: Any) -> Any:
                ret = func(*args, **vargs)
                setattr(self._obj, self._obj_name, self)
                return ret

            return wrapper
        return super(SakDbDict, self).__getattribute__(key)

    def __setitem__(self, key: _KT, item: _VT) -> None:
        super(SakDbDict, self).__setitem__(key, item)
        setattr(self._obj, self._obj_name, self)

    def __delitem__(self, key: _KT) -> None:
        super(SakDbDict, self).__delitem__(key)
        setattr(self._obj, self._obj_name, self)


class SakDbObject(object):
    namespace: SakDbNamespace
    key: str

    def __init__(
        self, namespace: "SakDbNamespace", key: Optional[str] = None, **kwargs: Any
    ) -> None:
        self.namespace = namespace
        if key is None:
            self.key = uuid.uuid4().hex
        else:
            self.key = key

        self.namespace.register_object(self)

        self._save()

        for name, value in kwargs.items():
            setattr(self, name, value)

    def _save(self) -> None:
        cl_payload = type(self).__name__
        cl_fields = SakDbFields(SakDbField(key="_cl", payload=cl_payload))

        previous_data = self.namespace.read(self.key, "_cl")
        if previous_data is not None:
            _cl_field = previous_data.get_by_key("_cl")
            if _cl_field is not None:
                if _cl_field.payload == cl_payload:
                    return

        self.namespace.write(self.key, "_cl", cl_fields)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_") or (name in ["namespace", "key"]):
            return super().__getattribute__(name)

        metadata_file = "meta"

        data = self.namespace.read(self.key, metadata_file)

        if data is None:
            raise Exception(f"{self} has no attribute {metadata_file}.")

        if self.namespace.graph is None:
            raise Exception(
                f"Namespace {self.namespace.name} must be attached to a graph."
            )

        decoder = SakDbDecoder(self.namespace.graph)

        type_field = data.get_by_key(f"_{name}:type")
        if type_field is None:
            raise Exception(f"Could not infere the type for {name}.")

        if type_field.payload == "list":
            # If the type is a list.
            tmp_list = []

            for field in data.fields:
                if not field.key.startswith(f"{name}:"):
                    continue

                value = json.loads(field.payload, object_hook=decoder.object_hook)
                tmp_list.append(value)

            return SakDbList(self, name, tmp_list)

        elif type_field.payload == "dict":
            # If the type is a dictionary.
            tmp_dict: Dict[str, Any] = {}

            for field in data.fields:
                if not field.key.startswith(f"{name}:"):
                    continue

                value = json.loads(field.payload, object_hook=decoder.object_hook)

                _, field_key = field.key.split(":", 1)
                tmp_dict[field_key] = value
            return SakDbDict(self, name, tmp_dict)

        else:
            # For all other types.
            object_field = data.get_by_key(name)

            if object_field is None:
                raise Exception(f"No attribute {name} for {self}.")

            value = json.loads(object_field.payload, object_hook=decoder.object_hook)
            return value

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or (name in ["namespace", "key"]):
            super(SakDbObject, self).__setattr__(name, value)
            return

        try:
            super(SakDbObject, self).__setattr__(name, value)

            if self.namespace.graph is None:
                raise Exception(
                    f"Namespace {self.namespace.name} must be attached to a graph."
                )

            encoder = SakDbEncoder()

            fields = []

            if isinstance(value, list):
                fields.append(SakDbField(key=f"_{name}:type", payload="list"))

                for idx, ivalue in enumerate(value):
                    payload_str = json.dumps(
                        ivalue, default=encoder.default, separators=(",", ":")
                    )
                    fields.append(
                        SakDbField(key=f"{name}:{str(idx)}", payload=payload_str)
                    )
            elif isinstance(value, dict):
                fields.append(SakDbField(key=f"_{name}:type", payload="dict"))

                for ikey, ivalue in value.items():
                    payload_str = json.dumps(
                        ivalue, default=encoder.default, separators=(",", ":")
                    )
                    fields.append(SakDbField(key=f"{name}:{ikey}", payload=payload_str))
            else:
                fields.append(
                    SakDbField(key=f"_{name}:type", payload=type(value).__name__)
                )

                payload_str = json.dumps(
                    value, default=encoder.default, separators=(",", ":")
                )
                fields.append(SakDbField(key=name, payload=payload_str))

            metadata_file = "meta"
            data = SakDbFields(*fields)

            previous_data = self.namespace.read(self.key, metadata_file)
            if previous_data is not None:
                previous_data.drop_by_key_prefix(f"_{name}:type")
                previous_data.drop_by_key_prefix(f"{name}:")
                new_data = merge(None, data, previous_data)
            else:
                new_data = data

            self.namespace.write(self.key, metadata_file, new_data)
        except Exception as e:
            raise (e)

    def __getitem__(self, name: str) -> Any:
        return self.__getattribute__(name)

    def __str__(self) -> str:
        return f"<{type(self).__name__} key: {self.key}>"
