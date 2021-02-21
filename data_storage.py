import uuid
from pathlib import Path
from dataclasses import dataclass, asdict, field
import json

from typing import List, Optional, Dict, Any, Set

from IPython import embed

import pygit2


class DataGraph(object):
    def __init__(self) -> None:
        super(DataGraph, self).__init__()

        self.namespaces: Dict[str, 'DataNamespace'] = {}
        self.classes: Dict[str, type] = {}

    def has_namespace_registered(self, name: str) -> bool:
        return name in self.namespaces

    def add_namepace(self, namespace: 'DataNamespace') -> None:
        if not self.has_namespace_registered(namespace.name):
            self.namespaces[namespace.name] = namespace
        namespace.set_graph(self)

    def save(self) -> None:
        for n in self.namespaces.values():
            n.save()

    def get_object(self, key: str) -> Optional['DataObject']:
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

    def get_objects(self) -> List['DataObject']:
        ret = []
        for n in self.namespaces.values():
            ret += list(n.get_objects())
        return ret


class NameSpaceWriter(object):
    def __init__(self) -> None:
        super(NameSpaceWriter, self).__init__()

    def node_keys(self) -> List[str]:
        return []

    def read(self, node_key: str, data_key: str) -> Optional[str]:
        return None

    def write(self, node_key: str, data_key: str, value: str) -> None:
        pass


class NameSpaceGitWriter(NameSpaceWriter):
    def __init__(self, path: Path, ref: str = 'refs/heads/master') -> None:
        super(NameSpaceGitWriter, self).__init__()

        self.repo = pygit2.Repository(path)
        self.ref = ref

        if self.ref not in self.repo.references:
            author = pygit2.Signature('a b', 'a@b')
            committer = author
            commit_message = 'Initial commit'

            index = pygit2.Index()
            tid = index.write_tree(self.repo)

            repo.create_commit(self.ref, author, committer, commit_message, tid, [])

    def node_keys(self) -> List[str]:
        # TODO: This should be a generator.
        ret = []

        ref = self.repo.references[self.ref].target
        tree = self.repo[ref].tree
        for obj in tree:
            # TODO: Use another way to check if it is a tree node.
            if obj.type_str == 'tree':
                for obj2 in self.repo[obj.id]:
                    ret.append(obj2.name)

        return ret

    def read(self, node_key: str, data_key: str) -> Optional[str]:
        ref = self.repo.references[self.ref].target
        tree = self.repo[ref].tree

        try:
            ret = tree['objects'][node_key[:2]][node_key][data_key].data.decode('utf-8')
            if isinstance(ret, str):
                return ret
            else:
                raise Exception("This should be a string")
        except:
            # TODO: Should only catch the non existing error.
            return None

    def write(self, node_key: str, data_key: str, value: str) -> None:
        node_path = Path('objects') / node_key[:2] / node_key
        data_path = node_path / data_key

        ref = self.repo.references[self.ref].target

        tree = self.repo[ref].tree

        index = pygit2.Index()

        index.read_tree(tree)

        blob = self.repo.create_blob(value)
        new_entry = pygit2.IndexEntry(str(data_path), blob, pygit2.GIT_FILEMODE_BLOB)

        index.add(new_entry)

        tid = index.write_tree(self.repo)

        diff = tree.diff_to_tree(self.repo[tid])

        if len(list(diff.deltas)):
            author = pygit2.Signature('a b', 'a@b')
            committer = author
            commit_message = 'Add new entry'

            oid = repo.create_commit(
                    self.ref,
                    author, committer, commit_message, tid, [ref])
            #TODO: What to do with this oid?


class DataNamespace(object):
    def __init__(self, graph: DataGraph, name: str, backend: 'NameSpaceWriter') -> None:
        super(DataNamespace, self).__init__()
        self.name = name

        self.backend = backend

        self.objects: Dict[str, 'DataObject'] = {}

        self.graph: Optional['DataGraph'] = None
        graph.add_namepace(self)

    def get_object_keys(self) -> Set[str]:
        #TODO: Use generators for this?
        return set(self.objects.keys()) | set(self.backend.node_keys())

    def get_objects(self) -> List['DataObject']:
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

    def get_object(self, key: str) -> 'DataObject':
        # TODO: Check if there is already this key in the graph, then return it
        # TODO: Otherwise create this object in the Namespace and return it

        if key in self.objects:
            return self.objects[key]

        # TODO: How do I properly choose the class. The idea is to have a field informing the class
        # of this object, then I instantiate this specific class.

        clname = None
        clname = self.backend.read(key, 'cl.txt')

        if clname is None:
            raise Exception(f'Failed to read the class type for object {key}')

        if self.graph is None:
            raise Exception(f'No graph registered!')
        else:
            cl = self.graph.get_class(clname)
            if cl is None:
                raise Exception(f'Class {clname} is not supported')
            else:
                obj = cl(self, key)

                if not isinstance(obj, DataObject):
                    raise Exception(f'Object {obj} should be an instance of DataObject')

                self.register_object(obj)
                return obj

    def has_object(self, key: str) -> bool:
        # TODO: I could check locally, otherwise also check in the filesystem
        return key in self.objects

    def register_object(self, obj: 'DataObject') -> None:
        if not self.has_object(obj.key):
            self.objects[obj.key] = obj


class DataObjectEncoder(json.JSONEncoder):
    def __init__(self, graph: 'DataGraph') -> None:
        super(DataObjectEncoder, self).__init__()
        self.graph = graph

    def default(self, value: Any) -> Any:
        if isinstance(value, DataObject):
            return {
                    '_type': 'DataObject',
                    'key': value.key
                    }
        else:
            return super().default(value)


class DataObjectDecoder(object):
    def __init__(self, graph: 'DataGraph') -> None:
        super(DataObjectDecoder, self).__init__()
        self.graph = graph

    def object_hook(self, value: Dict[str, Any]) -> Any:
        if '_type' in value:
            if value['_type'] == 'DataObject':
                return self.graph.get_object(value['key'])
        return value


class DataObject(object):
    namespace: DataNamespace
    key: str

    def __init__(self, namespace: 'DataNamespace', key: Optional[str] = None, **kwargs) -> None:
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
        self.namespace.backend.write(self.key, 'cl.txt', type(self).__name__)

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_') or (name in ['namespace', 'key']):
            value = super(DataObject, self).__getattr__(name)
            return value

        value_str = self.namespace.backend.read(self.key, name)

        if value_str is None:
            raise Exception(f'Failed to get value for {name}.')

        if self.namespace.graph is None:
            raise Exception(f'Namespace must be attached to a graph.')

        decoder = DataObjectDecoder(self.namespace.graph)
        value = json.loads(value_str, object_hook=decoder.object_hook)

        return value['value']

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith('_') or (name in ['namespace', 'key']):
            super(DataObject, self).__setattr__(name, value)
            return

        try:
            super(DataObject, self).__setattr__(name, value)

            outvalue = {
                    'type': type(value).__name__,
                    'value': value
                    }

            if self.namespace.graph is None:
                raise Exception(f'Namespace must be attached to a graph.')

            encoder = DataObjectEncoder(self.namespace.graph)

            self.namespace.backend.write(self.key, name, json.dumps(outvalue,
                default=encoder.default))

        except Exception as e:
            raise(e)

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def __str__(self) -> str:
        return f'<{type(self).__name__} key: {self.key}>'


class DataObject2(DataObject):
    name: str
    otherobj: DataObject
    foobar: List[int]
    hey: List[DataObject]

    def __str__(self) -> str:
        return f'<{type(self).__name__} key: {self.key} name: {repr(self.name)}>'


if __name__ == "__main__":
    g = DataGraph()

    nw = NameSpaceGitWriter(Path('data'), 'refs/heads/master')

    n = DataNamespace(g, 'data', nw)

    g.register_class(DataObject)
    g.register_class(DataObject2)

    a = DataObject(n, '1af852330e2c4e419c77923faf00f38c')

    b = DataObject2(n, key='60b20f9340894410b18133b53823a3f5')
    #b.name = 'Foo'
    #b.hello = 'world'
    #b.otherobj = a
    #
    #b.hey = [a, a, b]
    #b.foobar = [1]

    for o in g.get_objects():
        print(o)
