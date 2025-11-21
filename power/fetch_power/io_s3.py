# io_s3.py
# %%

from pathlib import Path
import uuid

def write_atomic(dst_path, data_bytes: bytes):
    """
    we use write_atomic() usually after parquet_to_bytes or whenever you 
    Atomic-ish write to a local file:
    write to tmp, then rename.
    """
    path = Path(dst_path) # get the path object representing dst_path (windows path)
    path.parent.mkdir(parents=True, exist_ok=True) 
    #creates the directory for the parent (including parent parent...:
    #  e.g. if we have data/id/data, it will create thsi directory but also data/id and data)
    tmp = path.with_name(path.name + f".tmp.{uuid.uuid4().hex}")  
    # with_name keeps the same path but changes the file's anme + and uuid.uuid4 generates a random unique id 
    with open(tmp, "wb") as f:
        f.write(data_bytes) 
        # .open() creates the file at the path tmp in binary and then f.write() 
        # will write the binary content from the paramter data_bytes
    tmp.replace(path) # here .replace() replaces the file at tmp by path 
    # ( and at the overwriting patyh if it already exists : such that when we get the latest data point we can overwrite the file before)

def list_paths(prefix):
    """
    Recursively list files under a given directory prefix (including all the files in the subfolders : so any file in this folder (including files in subfolders will eb returned))
    contrary to .glob() which will only return files in a given folder (and not seek files in the subfolders)
    """
    prefix_path = Path(prefix) #Path(prefix) returns path as a valid window Path (but it will not create a prefix file/verify if prefix exists)
    if not prefix_path.exists(): 
        return []
    return [str(p) for p in prefix_path.rglob("*") if p.is_file()] 


# %%
