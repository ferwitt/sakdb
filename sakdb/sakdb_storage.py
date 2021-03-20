import functools
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TypeVar

import pygit2

from sakdb.sakdb_fields import SakDbField, SakDbFields, merge, sakdb_dumps, sakdb_loads

VERSION = "0.0.1"

_T = TypeVar("_T")
_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class SakDbGraph(object):
    def __init__(self) -> None:
        super(SakDbGraph, self).__init__()

        self.namespaces: Dict[str, "SakDbNamespace"] = {}
        self.classes: Dict[str, type] = {}

    def has_namespace_registered(self, name: str) -> bool:
        return name in self.namespaces

    def add_namepace(self, namespace: "SakDbNamespace") -> None:
        if not self.has_namespace_registered(namespace.name):
            self.namespaces[namespace.name] = namespace
        namespace.set_graph(self)

    def save(self) -> None:
        for n in self.namespaces.values():
            n.save()

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


class SakDbNamespaceBackend(object):
    def __init__(self) -> None:
        super(SakDbNamespaceBackend, self).__init__()

    def set_metadata(self, key: str, value: Any) -> None:
        pass

    def get_metadata(self, key: str) -> Optional[Any]:
        return None

    def node_keys(self) -> List[str]:
        return []

    def read(self, node_key: str, data_key: str) -> Optional[SakDbFields]:
        return None

    def write(self, node_key: str, data_key: str, value: SakDbFields) -> None:
        pass


class SakDbNamespaceGit(SakDbNamespaceBackend):
    def __init__(self, path: Path, ref: str = "refs/heads/master") -> None:
        super(SakDbNamespaceGit, self).__init__()

        self.repo = pygit2.init_repository(path, True)
        self.ref = ref

        if self.ref not in self.repo.references:
            # author = pygit2.Signature("a b", "a@b")
            author = self.repo.default_signature
            committer = author
            commit_message = "Initial commit"

            index = pygit2.Index()
            tid = index.write_tree(self.repo)

            self.repo.create_commit(
                self.ref, author, committer, commit_message, tid, []
            )

    def add_remote(self, name: str, url: str) -> pygit2.remote.Remote:
        # TODO(witt): What is remote already exists?
        return self.repo.remotes.create(name, url)

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

            def do_merge(theirs_branch: pygit2.IndexEntry) -> None:
                theirs_commit = self.repo[theirs_branch.target]

                synced_branch = self.repo.branches[synced_branch_name]

                merge_result, _ = self.repo.merge_analysis(
                    theirs_branch.target, synced_branch.name
                )

                if merge_result & pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE:
                    return

                merged_index = self.repo.merge_commits(synced_branch, theirs_commit)

                if merged_index.conflicts:

                    for (base_index, ours_index, theirs_index) in list(
                        merged_index.conflicts
                    ):
                        path = None

                        base = None
                        if base_index is not None:
                            base = sakdb_loads(
                                self.repo[base_index.oid].data.decode("utf-8")
                            )
                            path = Path(base_index.path)
                        ours = None
                        if ours_index is not None:
                            ours = sakdb_loads(
                                self.repo[ours_index.oid].data.decode("utf-8")
                            )
                            path = Path(ours_index.path)
                        theirs = None
                        if theirs_index is not None:
                            theirs = sakdb_loads(
                                self.repo[theirs_index.oid].data.decode("utf-8")
                            )
                            path = Path(theirs_index.path)

                        merged = merge(base, ours, theirs)
                        if merged is None:
                            raise Exception(
                                "There was some unresolved merge conflict here."
                            )

                        merged_str = sakdb_dumps(merged)

                        blob = self.repo.create_blob(merged_str)
                        new_entry = pygit2.IndexEntry(
                            str(path), blob, pygit2.GIT_FILEMODE_BLOB
                        )
                        merged_index.add(new_entry)

                        del merged_index.conflicts[str(path)]

                user = self.repo.default_signature
                tree = merged_index.write_tree()
                message = "Merging branches"

                # TODO(witt): Is it necessary to move the synced branch?
                self.repo.create_commit(
                    synced_branch.name,
                    user,
                    user,
                    message,
                    tree,
                    [synced_branch.target, theirs_branch.target],
                )

            # Merge the synced branch with the local branch.
            do_merge(branch)

            # Merge the synced branch with all the remote synced branches.
            for remote_branch_name in self.repo.branches.remote:
                if not remote_branch_name.endswith(synced_branch_name):
                    continue
                remote_branch = self.repo.branches[remote_branch_name]
                do_merge(remote_branch)

            # Move the local branch to the synced branch.
            synced_branch = self.repo.branches[synced_branch_name]
            branch.set_target(synced_branch.target)

            for remote in self.repo.remotes:
                remote.push([synced_branch.name])
                remote.fetch()

    def set_metadata(self, key: str, value: Any) -> None:
        metada_path = Path("metadata") / key

        encoder = SakDbEncoder()
        payload_str = json.dumps(value, default=encoder.default)

        data = SakDbFields(
            SakDbField(key="_type", payload=type(value).__name__),
            SakDbField(key=key, payload=payload_str),
        )

        self._write_sakdb(metada_path, data)

    def get_metadata(self, key: str) -> Any:
        metada_path = Path("metadata") / key

        data = self._read_sakdb(metada_path)
        if data is None:
            raise Exception(f"Could not load the entry {key} from metadata DB")

        object_field = data.get_by_key(key)
        if object_field is None:
            raise Exception(f"No attribute {key} for {self}.")

        value = json.loads(object_field.payload)
        return value

    def node_keys(self) -> List[str]:
        # TODO: This should be a generator.
        ret = []

        ref = self.repo.references[self.ref].target
        tree = self.repo[ref].tree["objects"]
        for obj in tree:
            # TODO: Use another way to check if it is a tree node.
            if obj.type_str == "tree":
                for obj2 in self.repo[obj.id]:
                    ret.append(obj2.name)

        return ret

    def _read_blob(self, path: Path) -> Optional[pygit2.Object]:
        try:
            path_parts = path.parts

            ref = self.repo.references[self.ref].target
            tree = self.repo[ref].tree

            current_node = tree
            for ipath in path_parts:
                current_node = current_node[ipath]

            return current_node
        except Exception:
            # Exception("Could not read {path} in {self}.")
            return None

    def _read(self, path: Path) -> Optional[str]:
        blob = self._read_blob(path)

        if blob is not None:
            ret = blob.data.decode("utf-8")
            if isinstance(ret, str):
                return ret
            else:
                return None
        else:
            # Exception("Could not read {path} in {self}.")
            return None

    def _read_sakdb(self, path: Path) -> Optional[SakDbFields]:
        value_str = self._read(path)
        if value_str is None:
            return None
        return sakdb_loads(value_str)

    def read(self, node_key: str, data_key: str) -> Optional[SakDbFields]:
        node_path = Path("objects") / node_key[0] / node_key[1] / node_key[2] / node_key
        data_path = node_path / data_key
        return self._read_sakdb(data_path)

    def _write(self, path: Path, value: str) -> None:
        ref = self.repo.references[self.ref].target

        tree = self.repo[ref].tree

        blob = self.repo.create_blob(value)

        # Check if the blob is identical, then just return.
        previous_blob = self._read_blob(path)
        if previous_blob is not None:
            if blob == previous_blob.oid:
                return

        new_entry = pygit2.IndexEntry(str(path), blob, pygit2.GIT_FILEMODE_BLOB)

        index = pygit2.Index()
        index.read_tree(tree)
        index.add(new_entry)

        tid = index.write_tree(self.repo)

        # Old really slow way of comparing trees.
        # diff = tree.diff_to_tree(self.repo[tid])
        # if len(list(diff.deltas)):

        # If the tree id is different, then commit it.
        if tid != tree.id:
            # author = pygit2.Signature("a b", "a@b")
            author = self.repo.default_signature
            committer = author
            commit_message = "Add new entry"

            self.repo.create_commit(
                self.ref, author, committer, commit_message, tid, [ref]
            )

    def _write_sakdb(self, path: Path, value: SakDbFields) -> None:

        # Check if content changed, if not do not update the timestamps.
        prev_value = self._read_sakdb(path)
        if prev_value is not None:
            for prev_field in prev_value.fields:
                new_field = value.get_by_key(prev_field.key)

                if new_field is None:
                    continue

                if new_field.crc == prev_field.crc:
                    new_field.ts = prev_field.ts

        # Dump sanitized timestamp value to the Git.
        value_str = sakdb_dumps(value)
        self._write(path, value_str)

    def write(self, node_key: str, data_key: str, value: SakDbFields) -> None:
        node_path = Path("objects") / node_key[0] / node_key[1] / node_key[2] / node_key
        data_path = node_path / data_key
        self._write_sakdb(data_path, value)


class SakDbNamespace(object):
    def __init__(
        self, graph: SakDbGraph, name: str, backend: "SakDbNamespaceBackend"
    ) -> None:
        super(SakDbNamespace, self).__init__()
        self.name = name

        self.backend = backend

        self.objects: Dict[str, "SakDbObject"] = {}

        self.graph: Optional["SakDbGraph"] = None
        graph.add_namepace(self)

        # TODO: Read the version and check if it is compatible with the current implementation
        try:
            version = self.backend.get_metadata("version")
        except Exception:
            version = None

        if version is not None:
            if not self._validate_version(version):
                raise Exception(
                    f"Repo version ({version}) not supported, please update the system."
                )
        else:
            self.backend.set_metadata("version", VERSION)

    def _validate_version(self, version: str) -> bool:
        # Extract the repository version components.
        repo_version, _, _ = (int(v) for v in version.split("."))

        # Extract the current software version components.
        software_version, _, _ = (int(v) for v in VERSION.split("."))

        # If the major version number is greater then the supported version.
        if repo_version > software_version:
            return False
        return True

    def get_object_keys(self) -> Set[str]:
        # TODO: Use generators for this?
        return set(self.objects.keys()) | set(self.backend.node_keys())

    def get_objects(self) -> List["SakDbObject"]:
        ret = []
        for key in self.get_object_keys():
            ret.append(self.get_object(key))
        return ret

    def save(self) -> None:
        for obj in self.objects.values():
            obj.save()

    def set_graph(self, graph: SakDbGraph) -> None:
        self.graph = graph
        if not graph.has_namespace_registered(self.name):
            graph.add_namepace(self)

    def get_object(self, key: str) -> "SakDbObject":
        # Check if there is already this key in the graph, then return it.
        # Otherwise create this object in the Namespace and return it.
        if key in self.objects:
            return self.objects[key]

        # TODO: How do I properly choose the class. The idea is to have a field informing the class
        # of this object, then I instantiate this specific class.
        cl_fields = self.backend.read(key, "_cl")
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
        return self.backend.get_metadata("version")


class SakDbEncoder(json.JSONEncoder):
    def __init__(self) -> None:
        super(SakDbEncoder, self).__init__()

    def default(self, value: Any) -> Any:
        if isinstance(value, SakDbObject):
            return {"_type": "SakDbObject", "key": value.key}
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
        cl_fields = SakDbFields(SakDbField(key="_cl", payload=type(self).__name__))

        self.namespace.backend.write(self.key, "_cl", cl_fields)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_") or (name in ["namespace", "key"]):
            return super().__getattribute__(name)

        data = self.namespace.backend.read(self.key, name)

        if data is None:
            raise Exception(f"{self} has no attribute {name}.")

        if self.namespace.graph is None:
            raise Exception(
                f"Namespace {self.namespace.name} must be attached to a graph."
            )

        decoder = SakDbDecoder(self.namespace.graph)

        type_field = data.get_by_key("_type")
        if type_field is None:
            raise Exception(f"Could not infere the type for {name}.")

        if type_field.payload == "list":
            # If the type is a list.
            tmp_list = []

            for field in data.fields:
                if field.key.startswith("_"):
                    continue

                value = json.loads(field.payload, object_hook=decoder.object_hook)
                tmp_list.append(value)

            return SakDbList(self, name, tmp_list)

        elif type_field.payload == "dict":
            # If the type is a dictionary.
            tmp_dict: Dict[str, Any] = {}

            for field in data.fields:
                if field.key.startswith("_"):
                    continue

                value = json.loads(field.payload, object_hook=decoder.object_hook)
                tmp_dict[field.key] = value
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
                fields.append(SakDbField(key="_type", payload="list"))

                for idx, ivalue in enumerate(value):
                    payload_str = json.dumps(ivalue, default=encoder.default)
                    fields.append(SakDbField(key=str(idx), payload=payload_str))
            elif isinstance(value, dict):
                fields.append(SakDbField(key="_type", payload="dict"))

                for ikey, ivalue in value.items():
                    payload_str = json.dumps(ivalue, default=encoder.default)
                    fields.append(SakDbField(key=ikey, payload=payload_str))
            else:
                fields.append(SakDbField(key="_type", payload=type(value).__name__))

                payload_str = json.dumps(value, default=encoder.default)
                fields.append(SakDbField(key=name, payload=payload_str))

            data = SakDbFields(*fields)
            self.namespace.backend.write(self.key, name, data)
        except Exception as e:
            raise (e)

    def __getitem__(self, name: str) -> Any:
        return self.__getattribute__(name)

    def __str__(self) -> str:
        return f"<{type(self).__name__} key: {self.key}>"
