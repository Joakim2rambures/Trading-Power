# %%

import fsspec, uuid

def get_fs_and_url(url: str):
    # url example: "s3://my-bucket/parquet_root"
    fs, _, paths = fsspec.get_fs_token_paths(url)
    if isinstance(paths, list): path = paths[0]
    else: path = paths
    return fs, path.rstrip("/")

def write_atomic(fs, dst_path: str, data_bytes: bytes):
    # write to temp then move; for S3 this is effectively atomic at object level
    tmp = f"{dst_path}.tmp.{uuid.uuid4().hex}"
    with fs.open(tmp, "wb") as f:
        f.write(data_bytes)
    fs.mv(tmp, dst_path)  # overwrite if exists

def read_bytes_or_none(fs, path: str):
    if not fs.exists(path):
        return None
    with fs.open(path, "rb") as f:
        return f.read()

def list_paths(fs, prefix: str):
    return [p for p in fs.find(prefix) if not p.endswith("/")]