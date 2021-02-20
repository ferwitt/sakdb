import uuid
from pathlib import Path
from dataclasses import dataclass, asdict, field


class DataGraph(object):
    """docstring for DataGraph"""
    def __init__(self):
        super(DataGraph, self).__init__()

        self.namespaces = {}
        self.classes = {}

    def has_namespace_registered(self, name):
        return name in self.namespaces

    def add_namepace(self, namespace):
        if not self.has_namespace_registered(namespace.name):
            self.namespaces[namespace.name] = namespace
        namespace.set_graph(self)

    def save(self):
        for n in self.namespaces.values():
            n.save()

    def get_object(self, key):
        for n in self.namespaces.values():
            if n.has_object(key):
                return n.get_object(key)
        return None

    def register_class(self, cl):
        if cl.__name__ in self.classes:
            raise Exception(f"The class {cl.__name__} has been already registered")
        self.classes[cl.__name__] = cl

    def get_class(self, clname):
        return self.classes[clname]

    def get_objects(self):
        ret = []
        for n in self.namespaces.values():
            ret += list(n.get_objects())
        return ret


class DataNamespace(object):
    """docstring for DataNamespace"""
    def __init__(self, graph, name, path):
        super(DataNamespace, self).__init__()
        self.name = name
        #self.graph = graph
        self.path = Path(path)

        #self.graph.add_namepace(self)

        self.objects = {}

        self.graph = None
        graph.add_namepace(self)

    def get_object_keys(self):
        #TODO
        return set(self.objects.keys()) | set([x.name for x in  self.path.glob('*/*')] )

    def get_objects(self):
        ret = []
        for key in self.get_object_keys():
            ret.append(self.get_object(key))
        return ret

    def save(self):
        for obj in self.objects.values():
            obj.save()

    def set_graph(self, graph):
        self.graph = graph
        if not graph.has_namespace_registered(self.name):
            graph.add_namepace(self)

    def get_object(self, key):
        # TODO: Check if there is already this key in the graph, then return it
        # TODO: Otherwise create this object in the Namespace and return it

        if key in self.objects:
            return self.objects[key]

        # TODO: How do I properly choose the class. The idea is to have a field informing the class
        # of this object, then I instantiate this specific class.

        clname =  None
        with open(self.path / key[:2] / key / 'cl.txt', 'r') as f:
            clname = f.read()

        cl = self.graph.get_class(clname)
        obj = cl(self, key)
        self.register_object(obj)

        return obj

    def has_object(self, key):
        # TODO: I could check locally, otherwise also check in the filesystem
        return key in self.objects

    def register_object(self, obj):
        if not self.has_object(obj.key):
            self.objects[obj.key] = obj

        
#@dataclass
class DataObject:
    namespace: DataNamespace
    key: str = None

    """docstring for DataObject"""
    def __init__(self, namespace, key=None, **kwargs):
        #super(DataObject, self).__init__(namespace, key)

        # TODO(witt): Sanity check the key format.
        self.namespace = namespace
        if key is None:
            self.key = uuid.uuid4().hex
        else:
            self.key = key

        self.namespace.register_object(self)

        self._save()

        for name, value in kwargs.items():
            setattr(self, name, value)

    def _save(self):
        outpath = self.__path()
        outpath.mkdir(parents=True, exist_ok=True)

        # TODO: Find a way to not rewrite this all the time
        classfile = outpath / 'cl.txt'
        with open(classfile, 'w') as f:
            f.write(type(self).__name__)

    def __path(self):
        if self.key is None:
            self.key = uuid.uuid4().hex

        outpath = self.namespace.path / self.key[:2] / self.key
        outpath.mkdir(parents=True, exist_ok=True)
        return outpath

    def __getattr__(self, name):
        if name.startswith('_') or (name in ['namespace', 'key']):
            return super(DataObject, self).__getattr__(name)

        with open(self.__path() / name, 'r') as f:
            value = f.read()
            if value.startswith('DataObject:'):
                key = value.replace('DataObject:', '')
                return self.namespace.get_object(key)
            return value
        #return self.data.get(name, None)

    def __setattr__(self, name, value):
        if name.startswith('_') or (name in ['namespace', 'key']):
            super(DataObject, self).__setattr__(name, value)
            return

        try:
            super(DataObject, self).__setattr__(name, value)
            with open(self.__path() / name, 'w') as f:
                if isinstance(value, DataObject):
                    f.write(f'DataObject:{value.key}')
                else:
                    f.write(value)
        except Exception as e:
            raise(e)

    def __getitem__(self, name):
        import pdb; pdb.set_trace()
        if name.startswith('_') or (name in ['namespace', 'key']):
            return super(DataObject, self).__getitem__(name)

        with open(self.__path() / name, 'r') as f:
            value = f.read()
            if value.startswith('DataObject:'):
                key = value.replace('DataObject:', '')
                return self.namespace.get_object(key)
            return value
        #return self.data.get(name, None)

    def __str__(self):
        return f'<{type(self).__name__} key: {self.key}>'


#@dataclass
class DataObject2(DataObject):
    """docstring for DataObject2"""
    name: str
    otherobj: DataObject
    #def __init__(self, namespace, key=None):
    #    super(DataObject2, self).__init__(namespace, key)


    def __str__(self):
        return f'<{type(self).__name__} name: {repr(self.name)} otherobj: {self.otherobj}>'



g = DataGraph()
n = DataNamespace(g, 'data', 'data')

g.register_class(DataObject)
g.register_class(DataObject2)

#g.add_namepace(n)

#print(n.get_objects())


#a = DataObject(n, '1af852330e2c4e419c77923faf00f38c')
#a = DataObject(n)
#b = DataObject2(n)

#print(a)
#print(n.get_object(a.key))
#print(g.get_object(a.key))

#print(type(a))
#print(type(b))



#b = DataObject2(n, key='60b20f9340894410b18133b53823a3f5', name="hey")
#b.name = 'Foo'
#b.hello = 'world'
#b.otherobj = a
#print(b)

#c = DataObject2(n, key='60b20f9340894410b18133b53823a3f5')
#c.name = c.name.strip()
#print(c.name)
#print(c.hello)
#print(c.otherobj)

for o in g.get_objects():
    print(o)
