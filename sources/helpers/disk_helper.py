import os
import re
import logging

logger = logging.getLogger()

def list_dir(dir_path, rel_path, regex):
    try:
        dir_list = os.listdir(dir_path)
        
        ret = []        
        for i in dir_list:
            path = os.path.abspath(dir_path+'/'+i)
            print(path)

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