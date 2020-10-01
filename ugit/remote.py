import os

from . import base
from . import data


REMOTE_REFS_BASE = f'{data.REFS}/{data.HEADS}'
LOCAL_REFS_BASE = f'{data.REFS}/{data.REMOTE}'


def fetch(remote_path):
    refs = _get_remote_refs(remote_path, REMOTE_REFS_BASE)

    # update local refs to match server
    for remote_name, value in refs.items():
        refname = os.path.relpath(remote_name, REMOTE_REFS_BASE)
        data.update_ref(f'{LOCAL_REFS_BASE}/{refname}',
                        data.RefValue(symbolic=False, value=value))


def _get_remote_refs(remote_path, prefix=''):
    with data.change_git_dir(remote_path):
        return {refname: ref.value for refname, ref in data.iter_refs(prefix)}


def push(remote_path, refname):
    # get refs data
    remote_refs = _get_remote_refs(remote_path)
    remote_ref = remote_refs.get(refname)
    local_ref = data.get_ref(refname).value
    assert local_ref

    # don't allow force push
    assert not remote_ref or base.is_ancestor_of(local_ref, remote_ref)

    known_remote_refs = filter(data.object_exists, remote_refs)
    remote_objects = set(base.iter_objects_in_commits(known_remote_refs))
    local_objects = set(base.iter_objects_in_commits({local_ref}))
    objects_to_push = local_objects - remote_objects

    # push all missing objects
    for oid in objects_to_push:
        data.push_object(oid, remote_path)

    # update server ref to our value
    with data.change_git_dir(remote_path):
        data.update_ref(refname, data.RefValue(symbolic=False, value=local_ref))
