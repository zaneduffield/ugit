import hashlib
import os
import shutil

from collections import namedtuple
from contextlib import contextmanager

GIT_DIR = None


@contextmanager
def change_git_dir(new_dir):
    global GIT_DIR
    old_dir = GIT_DIR
    GIT_DIR = f'{new_dir}/.ugit' if new_dir != '.' else '.ugit'
    yield
    GIT_DIR = old_dir


BLOB_T = 'blob'
TREE_T = 'tree'
PARENT_T = 'parent'
COMMIT_T = 'commit'

HEAD = 'HEAD'
MERGE_HEAD = 'MERGE_HEAD'
MASTER = 'master'
HEAD_ALIASES = ['@']
HEADS = 'heads'
OBJECTS = 'objects'
REMOTE = 'remote'
REFS = 'refs'
TAGS = 'tags'

SYMBOLIC_REF_PREFIX = 'ref: '


def init():
    os.makedirs(GIT_DIR)
    os.makedirs(f'{GIT_DIR}/{OBJECTS}')


RefValue = namedtuple('RefValue', ['symbolic', 'value'])


def update_ref(ref, value, deref=True):
    ref = _get_ref_internal(ref, deref)[0]

    assert value.value
    if value.symbolic:
        value = f'{SYMBOLIC_REF_PREFIX}{value.value}'
    else:
        value = value.value

    ref_path = f'{GIT_DIR}/{ref}'
    os.makedirs(os.path.dirname(ref_path), exist_ok=True)
    with open(ref_path, 'w') as f:
        f.write(value)


def get_ref(ref, deref=True):
    return _get_ref_internal(ref, deref=deref)[1]


def delete_ref(ref, deref=True):
    ref = _get_ref_internal(ref, deref)[0]
    os.remove(f'{GIT_DIR}/{ref}')


def _get_ref_internal(ref, deref):
    ref_path = f'{GIT_DIR}/{ref}'
    value = None
    if os.path.isfile(ref_path):
        with open(ref_path) as f:
            value = f.read().strip()

    symbolic = bool(value) and value.startswith(SYMBOLIC_REF_PREFIX)
    if symbolic:
        value = value.split(':', 1)[1].strip()
        if deref:
            return _get_ref_internal(value, deref=True)

    return ref, RefValue(symbolic=symbolic, value=value)


def iter_refs(prefix='', deref=True):
    refs = [HEAD, MERGE_HEAD]
    for root, _, filenames in os.walk(f'{GIT_DIR}/{REFS}/'):
        root = os.path.relpath(root, GIT_DIR)
        refs.extend(os.path.join(root, name).replace('\\', '/') for name in filenames)

    for refname in refs:
        if prefix and not os.path.normpath(refname).startswith(os.path.normpath(prefix)):
            continue
        ref = get_ref(refname, deref=deref)
        if ref.value:
            yield refname, ref


def hash_object(data, type_=BLOB_T):
    obj = type_.encode() + b'\x00' + data
    oid = hashlib.sha1(obj).hexdigest()
    with open(f'{GIT_DIR}/{OBJECTS}/{oid}', 'wb') as out:
        out.write(obj)
    return oid


def get_object(oid, expected=BLOB_T):
    with open(f'{GIT_DIR}/{OBJECTS}/{oid}', 'rb') as f:
        obj = f.read()

    first_null = obj.index(b'\x00')
    type_ = obj[:first_null].decode()
    content = obj[first_null + 1:]

    if expected is not None:
        assert type_ == expected, f'Expected {expected}, got {type_}'

    return content


def object_exists(oid):
    return os.path.isfile(f'{GIT_DIR}/{OBJECTS}/{oid}')


def fetch_object_if_missing(oid, remote_git_dir):
    if object_exists(oid):
        return
    remote_git_dir += '/.ugit'
    shutil.copy(f'{remote_git_dir}/{OBJECTS}/{oid}',
                f'{GIT_DIR}/{OBJECTS}/{oid}')


def push_object(oid, remote_git_dir):
    remote_git_dir = '/.ugit'
    shutil.copy(f'{GIT_DIR}/{OBJECTS}/{oid}',
                f'{remote_git_dir}/{OBJECTS}/{oid}')
