import itertools
import operator
import os
import string

from collections import deque, namedtuple

from . import data
from . import diff


def init():
    data.init()
    data.update_ref(data.HEAD, data.RefValue(symbolic=True, value=f'{data.REFS}/{data.HEADS}/{data.MASTER}'))


def write_tree(directory='.'):
    # index is flat, we need it as a tree of dicts
    index_as_tree = {}
    with data.get_index() as index:
        for path, oid in index.items():
            path = path.split('/')
            dirpath, filename = path[:-1], path[-1]

            current = index_as_tree
            # find the dict for the directory of this file
            for dirname in dirpath:
                current = current.setdefault(dirname, {})
            current[filename] = oid

    def write_tree_recursive(tree_dict):
        entries = []
        for name, value in tree_dict.items():
            if type(value) is dict:
                type_ = data.TREE_T
                oid = write_tree_recursive(value)
            else:
                type_ = data.BLOB_T
                oid = value
            entries.append((name,  oid, type_))

        tree = ''.join(f'{type_} {oid} {name}\n'
                       for name, oid, type_
                       in sorted(entries))
        return data.hash_object(tree.encode(), data.TREE_T)

    return write_tree_recursive(index_as_tree)


def _iter_tree_entries(oid):
    if not oid:
        return
    tree = data.get_object(oid, data.TREE_T)
    for entry in tree.decode().splitlines():
        type_, oid, name = entry.split(' ', 2)
        yield type_, oid, name


def get_tree(oid, base_path=''):
    result = {}
    for type_, oid, name in _iter_tree_entries(oid):
        assert '/' not in name
        assert name not in ('..', '.')
        path = base_path + name
        if type_ == data.BLOB_T:
            result[path] = oid
        elif type_ == data.TREE_T:
            result.update(get_tree(oid, f'{path}/'))
        else:
            raise Exception(f'Unknown tree entry {type_}')
    return result


def get_working_tree():
    result = {}
    for root, _, filenames in os.walk('.'):
        for filename in filenames:
            path = os.path.relpath(f'{root}/{filename}')
            if is_ignored(path) or not os.path.isfile(path):
                continue
            with open(path, 'rb') as f:
                result[path] = data.hash_object(f.read())

    return result


def get_index_tree():
    with data.get_index() as index:
        return index


def _emtpy_current_directory():
    for root, _, filenames in os.walk('.'):
        for filename in filenames:
            path = os.path.relpath(f'{root}/{filename}')
            if is_ignored(path) or not os.path.isfile(path):
                continue
            os.remove(path)


def read_tree(tree_oid, update_working=False):
    with data.get_index() as index:
        index.clear()
        index.update(get_tree(tree_oid))

        if update_working:
            _checkout_index(index)


def read_tree_merged(t_base, t_HEAD, t_other, update_working=False):
    with data.get_index() as index:
        index.clear()
        index.update(diff.merge_trees(
            get_tree(t_base),
            get_tree(t_HEAD),
            get_tree(t_other)
        ))

        if update_working:
            _checkout_index(index)


def _checkout_index(index):
    _emtpy_current_directory()
    for path, oid in index.items():
        os.makedirs(os.path.dirname(f'./{path}'), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data.get_object(oid, data.BLOB_T))


def commit(message):
    commit = f'{data.TREE_T} {write_tree()}\n'

    HEAD = data.get_ref(data.HEAD).value
    if HEAD:
        commit += f'{data.PARENT_T} {HEAD}\n'
    MERGE_HEAD = data.get_ref(data.MERGE_HEAD).value
    if MERGE_HEAD:
        commit += f'{data.PARENT_T} {MERGE_HEAD}\n'
        data.delete_ref(data.MERGE_HEAD, deref=False)

    commit += '\n'
    commit += f'{message}\n'

    oid = data.hash_object(commit.encode(), data.COMMIT_T)

    data.update_ref(data.HEAD, data.RefValue(symbolic=False, value=oid))
    return oid


def checkout(name):
    oid = get_oid(name)
    commit = get_commit(oid)
    read_tree(commit.tree)

    if is_branch(name):
        HEAD = data.RefValue(symbolic=True, value=f'{data.REFS}/{data.HEADS}/{name}')
    else:
        HEAD = data.RefValue(symbolic=False, value=oid)

    data.update_ref(data.HEAD, HEAD, deref=False)


def reset(oid):
    # deref = True means that the underlying value of HEAD will be changed as a result
    data.update_ref(data.HEAD, data.RefValue(symbolic=False, value=oid))


def merge(other):
    HEAD = data.get_ref(data.HEAD).value
    assert HEAD
    merge_base = get_merge_base(other, HEAD)
    c_other = get_commit(other)

    # handle fast-forward merge
    if merge_base == HEAD:
        read_tree(c_other.tree)
        data.update_ref(data.HEAD,
                        data.RefValue(symbolic=False, value=other)
                        )
        print('Fase-forward merge, no need to commit')
        return

    data.update_ref(data.MERGE_HEAD, data.RefValue(symbolic=False, value=other))

    c_base = get_commit(merge_base)
    c_HEAD = get_commit(HEAD)
    read_tree_merged(c_base.tree, c_HEAD.tree, c_other.tree)
    print('Merged in working tree\nPlease commit')


def get_merge_base(oid1, oid2):
    parents1 = set(iter_commits_and_parents({oid1}))

    for oid in iter_commits_and_parents({oid2}):
        if oid in parents1:
            return oid


def is_ancestor_of(commit, maybe_ancestor):
    return maybe_ancestor in iter_commits_and_parents({commit})


def create_tag(name, oid):
    data.update_ref(f'{data.REFS}/{data.TAGS}/{name}', data.RefValue(symbolic=False, value=oid))


def iter_branch_names():
    for refname, _ in data.iter_refs(f'{data.REFS}/{data.HEADS}/'):
        yield os.path.relpath(refname, f'{data.REFS}/{data.HEADS}/')


def is_branch(branch):
    return data.get_ref(f'{data.REFS}/{data.HEADS}/{branch}').value is not None


def create_branch(name, oid):
    data.update_ref(f'{data.REFS}/{data.HEADS}/{name}', data.RefValue(symbolic=False, value=oid))


def get_branch_name():
    HEAD = data.get_ref(data.HEAD, deref=False)
    if not HEAD.symbolic:
        return None

    HEAD = HEAD.value
    assert HEAD.startswith(f'{data.REFS}/{data.HEADS}/')
    return os.path.relpath(HEAD, f'{data.REFS}/{data.HEADS}')


Commit = namedtuple('Commit', ['tree', 'parents', 'message'])


def get_commit(oid):
    parents = []
    
    commit = data.get_object(oid, data.COMMIT_T).decode()
    lines = iter(commit.splitlines())
    for line in itertools.takewhile(operator.truth, lines):
        key, value = line.split(' ', 1)
        if key == data.TREE_T:
            tree = value
        elif key == data.PARENT_T:
            parents.append(value)
        else:
            raise Exception(f'Unknown field {key}')

    message = '\n'.join(lines)
    return Commit(tree=tree, parents=parents, message=message)
    

def iter_commits_and_parents(oids):
    oids = deque(oids)
    visited = set()

    while oids:
        oid = oids.popleft()
        if not oid or oid in visited:
            continue

        visited.add(oid)
        yield oid

        commit = get_commit(oid)
        # return first parent next
        oids.extendleft(commit.parents[:1])
        # return other parents later
        oids.extend(commit.parents[1:])


def iter_objects_in_commits(oids):
    # must yield the oid before accessing it (to allow caller to fetch it first if needed)
    visited = set()

    def iter_objects_in_tree(oid):
        visited.add(oid)
        yield oid
        for type_, oid, _ in _iter_tree_entries(oid):
            if oid not in visited:
                if type_ == data.TREE_T:
                    yield from iter_objects_in_tree(oid)
                else:
                    visited.add(oid)
                    yield oid

    for oid in iter_commits_and_parents(oids):
        yield oid
        commit = get_commit(oid)
        if commit.tree not in visited:
            yield from iter_objects_in_tree(commit.tree)


def get_oid(name):
    if name in data.HEAD_ALIASES:
        name = data.HEAD

    # Name is ref
    refs_to_try = [
        name,
        f'{data.REFS}/{name}',
        f'{data.REFS}/{data.TAGS}/{name}',
        f'{data.REFS}/{data.HEADS}/{name}',
    ]
    for ref in refs_to_try:
        if data.get_ref(ref, deref=False).value:
            return data.get_ref(ref).value

    # Name is SHA1
    is_hex = all(c in string.hexdigits for c in name)
    if len(name) == 40 and is_hex:
        return name

    raise Exception(f'Unknown name {name}')


def add(filenames):

    def add_file(filename):
        # normalise path
        filename = os.path.relpath(filename)
        with open(filename, 'fb') as f:
            oid = data.hash_object(f.read())
        index[filename] = oid

    def add_directory(dirname):
        for root, _, filenames in os.walk(dirname):
            for filename in filenames:
                path = os.path.relpath(f'{root}/{filename}')
                if is_ignored(path) or not os.path.isfile(path):
                    continue
                add_file(path)

    with data.get_index() as index:
        for name in filenames:
            if os.path.isfile(name):
                add_file(name)
            elif os.path.isdir(name):
                add_directory(name)


def is_ignored(path: str):
    return data.GIT_DIR in path.split('/') or data.GIT_DIR in path.split('\\')
