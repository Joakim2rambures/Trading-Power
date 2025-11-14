# %%

from pathlib import Path
import uuid

def write_atomic(dst_path, data_bytes: bytes):
    """
    Atomic-ish write to a local file:
    write to tmp, then rename.
    """
    path = Path(dst_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{uuid.uuid4().hex}")
    with open(tmp, "wb") as f:
        f.write(data_bytes)
    tmp.replace(path)

def list_paths(prefix):
    """
    Recursively list files under a given directory prefix.
    """
    prefix_path = Path(prefix)
    if not prefix_path.exists():
        return []
    return [str(p) for p in prefix_path.rglob("*") if p.is_file()]