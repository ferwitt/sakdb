import uuid
from pathlib import Path
import json
import datetime

from typing import List, Optional, Dict, Any, Set

from data_field import data_object_loads, data_object_dumps, DataObjectFields, DataObjectField, PAYLOAD_SEPARATOR, merge

import pygit2

VERSION = "0.0.1"


class DataGraph(object):
    def __init__(self) -> None:
        super(DataGraph, self).__init__()

        self.namespaces: Dict[str, "DataNamespace"] = {}
        self.classes: Dict[str, type] = {}

    def has_namespace_registered(self, name: str) -> bool:
        return name in self.namespaces

    def add_namepace(self, namespace: "DataNamespace") -> None:
        if not self.has_namespace_registered(namespace.name):
            self.namespaces[namespace.name] = namespace
        namespace.set_graph(self)

    def save(self) -> None:
        for n in self.namespaces.values():
            n.save()

    def get_object(self, key: str) -> Optional["DataObject"]:
        for n in self.namespaces.values():
            if n.has_object(key):
                return n.get_object(key)
        return None

    def register_class(self, cl: type) -> None:
        if cl.__name__ in self.classes:
            raise Exception(f"The class {cl.__name__} has been already registered")
        self.classes[cl.__name__] = cl

    def get_class(self, clname: str) -> type:
        return self.classes[clname]

    def get_objects(self) -> List["DataObject"]:
        ret = []
        for n in self.namespaces.values():
            ret += list(n.get_objects())
        return ret


class NameSpaceWriter(object):
    def __init__(self) -> None:
        super(NameSpaceWriter, self).__init__()

    def set_metadata(self, key: str, value: Any) -> None:
        pass

    def get_metadata(self, key: str) -> Any:
        return None

    def node_keys(self) -> List[str]:
        return []

    def read(self, node_key: str, data_key: str) -> Optional[str]:
        return None

    def write(self, node_key: str, data_key: str, value: str) -> None:
        pass


class NameSpaceGitWriter(NameSpaceWriter):
    def __init__(self, path: Path, ref: str = "refs/heads/master") -> None:
        super(NameSpaceGitWriter, self).__init__()

        self.repo = pygit2.init_repository(path, True)

        #try:
        #    self.repo = pygit2.Repository(path)
        #except pygit2.GitError as e:
        #    print(repr(e))
        #    raise(e)
        self.ref = ref

        if self.ref not in self.repo.references:
            #author = pygit2.Signature("a b", "a@b")
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

    def sync(self, remotes=[]) -> None:
        for remote in self.repo.remotes:
            remote.fetch()

        for branch_name in self.repo.branches.local:
            branch = self.repo.branches[branch_name]

            if branch_name.startswith('synced/'):
                continue

            # I will create the sync branch.
            synced_branch_name = f'synced/{branch_name}'
            if synced_branch_name not in self.repo.branches:
                branch_commit = self.repo[branch.target]
                self.repo.branches.local.create(synced_branch_name, branch_commit)

            def do_merge(theirs_branch):
                theirs_commit = self.repo[theirs_branch.target]


                synced_branch = self.repo.branches[synced_branch_name]
                synced_branch_coomit = self.repo[synced_branch.target]

                synced_branch_reference = self.repo.lookup_branch

                #import pdb; pdb.set_trace()

                merge_result, _ = self.repo.merge_analysis(theirs_branch.target, synced_branch.name)

                if merge_result & pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE:
                    return

                #from IPython import embed; embed()

                merged_index = self.repo.merge_commits(synced_branch, theirs_commit)

                if merged_index.conflicts:

                    resolved_conflict_idx = []
                    for conflict_idx, indexes  in enumerate(list(merged_index.conflicts)):
                        #import pdb; pdb.set_trace()

                        path = None
                        (base_index, ours_index, theirs_index) = indexes

                        base= None
                        if base_index is not None:
                            base = data_object_loads(self.repo[base_index.oid].data.decode('utf-8'))
                            path = Path(base_index.path)
                        ours = None
                        if ours_index is not None:
                            ours = data_object_loads(self.repo[ours_index.oid].data.decode('utf-8'))
                            path = Path(ours_index.path)
                        theirs = None
                        if theirs_index is not None:
                            theirs = data_object_loads(self.repo[theirs_index.oid].data.decode('utf-8'))
                            path = Path(theirs_index.path)

                        merged = merge(base, ours, theirs)
                        if merged is None:
                            raise Exception("There was some unresolved merge conflict here.")

                        merged_str = data_object_dumps(merged)

                        #from IPython import embed; embed()
                        #import pdb; pdb.set_trace()

                        #merged_index.remove(str(path))

                        # TODO(witt): What is the path?
                        blob = self.repo.create_blob(merged_str)
                        new_entry = pygit2.IndexEntry(str(path), blob, pygit2.GIT_FILEMODE_BLOB)
                        merged_index.add(new_entry)

                        del merged_index.conflicts[str(path)]


                user = self.repo.default_signature
                tree = merged_index.write_tree()
                message = "Merging branches"

                # TODO(witt): Is it necessary to move the synced branch?
                new_commit = self.repo.create_commit(synced_branch.name, user, user, message, tree,
                        [synced_branch.target, theirs_branch.target])

                #from IPython import embed; embed()
                #synced_branch.set_target(new_commit)



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
        self._write(metada_path, json.dumps(value, indent=1))

    def get_metadata(self, key: str) -> Any:
        metada_path = Path("metadata") / key
        value = self._read(metada_path)
        if value is not None:
            return json.loads(value)
        return None

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

    def _read(self, path: Path) -> Optional[str]:
        try:
            path_parts = path.parts

            ref = self.repo.references[self.ref].target
            tree = self.repo[ref].tree

            current_node = tree
            for ipath in path_parts:
                current_node = current_node[ipath]

            ret = current_node.data.decode("utf-8")
            if isinstance(ret, str):
                return ret
            else:
                return None
        except Exception as e:
            print("Could not read path", path, str(e))
            return None

    def read(self, node_key: str, data_key: str) -> Optional[str]:
        node_path = Path("objects") / node_key[:2] / node_key
        data_path = node_path / data_key

        ret = self._read(data_path)
        if isinstance(ret, str):
            return ret
        else:
            return None
            # raise Exception("This should be a string")

    def _write(self, path: Path, value: str) -> None:
        ref = self.repo.references[self.ref].target

        tree = self.repo[ref].tree

        index = pygit2.Index()
        index.read_tree(tree)

        blob = self.repo.create_blob(value)
        new_entry = pygit2.IndexEntry(str(path), blob, pygit2.GIT_FILEMODE_BLOB)

        index.add(new_entry)

        tid = index.write_tree(self.repo)

        diff = tree.diff_to_tree(self.repo[tid])

        if len(list(diff.deltas)):
            #author = pygit2.Signature("a b", "a@b")
            author = self.repo.default_signature
            committer = author
            commit_message = "Add new entry"

            self.repo.create_commit(
                self.ref, author, committer, commit_message, tid, [ref]
            )

    def write(self, node_key: str, data_key: str, value: str) -> None:
        node_path = Path("objects") / node_key[:2] / node_key
        data_path = node_path / data_key

        self._write(data_path, value)


class DataNamespace(object):
    def __init__(self, graph: DataGraph, name: str, backend: "NameSpaceWriter") -> None:
        super(DataNamespace, self).__init__()
        self.name = name

        self.backend = backend

        self.objects: Dict[str, "DataObject"] = {}

        self.graph: Optional["DataGraph"] = None
        graph.add_namepace(self)

        # TODO: Read the version and check if it is compatible with the current implementation
        self.backend.set_metadata("version", VERSION)

    def get_object_keys(self) -> Set[str]:
        # TODO: Use generators for this?
        return set(self.objects.keys()) | set(self.backend.node_keys())

    def get_objects(self) -> List["DataObject"]:
        ret = []
        for key in self.get_object_keys():
            ret.append(self.get_object(key))
        return ret

    def save(self) -> None:
        for obj in self.objects.values():
            obj.save()

    def set_graph(self, graph: DataGraph) -> None:
        self.graph = graph
        if not graph.has_namespace_registered(self.name):
            graph.add_namepace(self)

    def get_object(self, key: str) -> "DataObject":
        # TODO: Check if there is already this key in the graph, then return it
        # TODO: Otherwise create this object in the Namespace and return it

        if key in self.objects:
            return self.objects[key]

        # TODO: How do I properly choose the class. The idea is to have a field informing the class
        # of this object, then I instantiate this specific class.

        clname = None
        clname = self.backend.read(key, "cl.txt")

        if clname is None:
            raise Exception(f"Failed to read the class type for object {key}")

        if self.graph is None:
            raise Exception(f"No graph registered for namespace {self.name}!")
        else:
            cl = self.graph.get_class(clname)
            if cl is None:
                raise Exception(f"Class {clname} is not supported")
            else:
                obj = cl(self, key)

                if not isinstance(obj, DataObject):
                    raise Exception(f"Object {obj} should be an instance of DataObject")

                self.register_object(obj)
                return obj

    def has_object(self, key: str) -> bool:
        # TODO: I could check locally, otherwise also check in the filesystem
        return key in self.objects

    def register_object(self, obj: "DataObject") -> None:
        if not self.has_object(obj.key):
            self.objects[obj.key] = obj

    def get_version(self) -> str:
        return self.backend.get_metadata('version')


class DataObjectEncoder(json.JSONEncoder):
    def __init__(self, *args, **vargs) -> None:
        super(DataObjectEncoder, self).__init__(*args, **vargs)
        #self.graph = graph

    def default(self, value: Any) -> Any:
        if isinstance(value, DataObject):
            # TODO(witt): Add some more metadata here? If I add the clock, I can use this to solve
            # merge conflicts.
            return {"_type": "DataObject", "key": value.key}
        else:
            return super(DataObjectEncoder, self).default(val)


class DataObjectDecoder(object):
    def __init__(self, graph: "DataGraph") -> None:
        super(DataObjectDecoder, self).__init__()
        self.graph = graph

    def object_hook(self, value: Dict[str, Any]) -> Any:
        if "_type" in value:
            if value["_type"] == "DataObject":
                return self.graph.get_object(value["key"])
        return value





class DataObject(object):
    namespace: DataNamespace
    key: str

    def __init__(
        self, namespace: "DataNamespace", key: Optional[str] = None, **kwargs: Any
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
        self.namespace.backend.write(self.key, "cl.txt", type(self).__name__)

    def __getattr__structure__(self, name: str) -> Dict[str, Any]:
        pass


    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_") or (name in ["namespace", "key"]):
            return super().__getattribute__(name)

        value_str = self.namespace.backend.read(self.key, name)

        if value_str is None:
            # TODO: Should this thing here fail?
            # raise Exception(f'Failed to get value for {name}.')
            return None

        if self.namespace.graph is None:
            raise Exception(
                f"Namespace {self.namespace.name} must be attached to a graph."
            )

        decoder = DataObjectDecoder(self.namespace.graph)

        data = data_object_loads(value_str)

        value = json.loads(data.get_by_key(name).payload, object_hook=decoder.object_hook)
        return value

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or (name in ["namespace", "key"]):
            super(DataObject, self).__setattr__(name, value)
            return

        try:
            super(DataObject, self).__setattr__(name, value)


            if self.namespace.graph is None:
                raise Exception(
                    f"Namespace {self.namespace.name} must be attached to a graph."
                )

            encoder = DataObjectEncoder(
                    #self.namespace.graph
                    )
            payload_str = json.dumps(value, default=encoder.default)

            data = DataObjectFields(
                    type_name = type(value).__name__,
                    fields=[
                        DataObjectField(
                            key=name,
                            payload = payload_str
                            )
                        ]
                    )

            outvalue_str = data_object_dumps(data)

            self.namespace.backend.write( self.key, name, outvalue_str) 
        except Exception as e:
            raise (e)

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def __str__(self) -> str:
        return f"<{type(self).__name__} key: {self.key}>"


class DataObject2(DataObject):
    name: str
    otherobj: DataObject
    foobar: List[int]
    hey: List[DataObject]

    def __str__(self) -> str:
        return f"<{type(self).__name__} key: {self.key} name: {repr(self.name)}>"


if __name__ == "__main__":
    g = DataGraph()

    nw = NameSpaceGitWriter(Path("data"), "refs/heads/master")

    n = DataNamespace(g, "data", nw)

    g.register_class(DataObject)
    g.register_class(DataObject2)

    a = DataObject(n, "1af852330e2c4e419c77923faf00f38c")

    b = DataObject2(n, key="60b20f9340894410b18133b53823a3f5")
    b.name = 'Foo'
    b.hello = 'world'
    b.otherobj = a
    
    b.hey = [a, a, b]
    b.foobar = [1]

    for o in g.get_objects():
        print(o)
