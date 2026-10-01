import os
import re
import logging

logger = logging.getLogger()

def resolve_under_root(root, *parts):
    """Resolve a relative path, rejecting traversal and symlinks outside root."""
    if not root or any(not isinstance(part, str) or os.path.isabs(part) for part in parts):
        raise ValueError("not allowed")
    real_root = os.path.realpath(root)
    target = os.path.realpath(os.path.join(real_root, *parts))
    if os.path.commonpath([real_root, target]) != real_root:
        raise ValueError("not allowed")
    return target


def can_access_file_app(app, user):
    privileges = app.get("privileges", [])
    user_privileges = user.get("privileges", [])
    return "admin" in user_privileges or not privileges or any(
        privilege in privileges for privilege in user_privileges
    )


def can_access_logs(user):
    return "admin" in user.get("privileges", []) or "logs" in user.get("privileges", [])


def get_all_file_paths(directory, allowed_root):
    file_paths = []
    for root, directories, files in os.walk(directory):
        directories[:] = [name for name in directories
                          if is_path_within_roots(os.path.join(root, name), [allowed_root])]
        for filename in files:
            filepath = os.path.join(root, filename)
            if is_path_within_roots(filepath, [allowed_root]):
                file_paths.append(filepath)
    return file_paths


def list_dir(dir_path, rel_path, regex, allowed_root=None):
    try:
        dir_list = os.listdir(dir_path)
        
        ret = []        
        for i in dir_list:
            path = os.path.abspath(dir_path+'/'+i)
            if allowed_root is not None and not is_path_within_roots(path, [allowed_root]):
                continue

            stats = os.stat(path)
            obj_name = path.split('/')[-1]

            extension = 'dir'
            obj_type="na"
            if os.path.isfile(path):
                obj_type = 'file'

                if regex != '':
                    try:
                        z = re.match(regex, obj_name)
                    except:
                        continue

                    if z is None:
                        continue

                extension = obj_name.split('.')[-1]

            if os.path.isdir(path):
                obj_type = 'dir'

            obj = {
                'path' : (rel_path+'/'+i).replace('//','/'),
                'creation_time' : int(stats.st_ctime),
                'modification_time' : int(stats.st_mtime),
                'name' : obj_name,
                'type' : obj_type,
                'size' : stats.st_size,
                'extension' : extension 
            }

            ret.append(obj)
                
        return {'error':"", 'data':ret}
            
    except FileNotFoundError:
        logger.error(f"the directory {dir_path} doesnt exist")
        return {'error':"the directory doesnt exist"}
    except NotADirectoryError:
        logger.error(f"{dir_path} is not a directory")
        return {'error':"not a directory"}


def is_path_within_roots(path, roots):
    """Return True only if `path` resolves to a location inside one of `roots`.

    Symlinks and `..` segments are resolved with os.path.realpath before the
    comparison, so a symlink pointing outside an allowed root is rejected.
    """
    try:
        real_path = os.path.realpath(path)
    except (OSError, ValueError):
        return False

    for root in roots:
        if not root:
            continue
        try:
            real_root = os.path.realpath(root)
            if os.path.commonpath([real_path, real_root]) == real_root:
                return True
        except (OSError, ValueError):
            # Different drives on Windows or an invalid root: ignore it.
            continue

    return False
