from pathlib import Path
from typing import List

from sakdb import SakDbGraph, SakDbNamespaceGit, SakDbObject


class Task(SakDbObject):
    text: str
    done: bool


class TodoList(SakDbObject):
    tasks: List[Task]


###############################################################################
# Repository 1
###############################################################################
# Create a graph.
g1 = SakDbGraph()

# Register the classes supported by the database.
g1.register_class(Task)
g1.register_class(TodoList)

# Create a namespace backed by Git.
n1 = SakDbNamespaceGit(g1, "data", Path("/tmp/todolist"), "master")

with g1.session(msg="Add tasks") as s:
    # Create a TODO list on namespace.
    todo = TodoList(n1, tasks=[])
    s.commit("Add todo object")

    # Add item to the todo list.
    todo_item1 = Task(n1, text="Do something", done=False)
    todo_item2 = Task(n1, text="Do another thing", done=True)
    todo.tasks.append(todo_item1)
    todo.tasks.append(todo_item2)

# Sync database content.
n1.sync()

###############################################################################
# Repository 2
###############################################################################
# Create another graph and namespace repository.
g2 = SakDbGraph()
g2.register_class(Task)
g2.register_class(TodoList)

n2 = SakDbNamespaceGit(g2, "data", Path("/tmp/todolist_replicate"), "master")

# Link the other repository and synchronize it.
n2.add_remote("todolist", "/tmp/todolist")
n2.sync()

# Print task list on replicated database.
todo2 = g2.get_object(todo.key)
if todo2:
    for task in todo2.tasks:
        print(f"{task.text} is done -> {task.done}")
