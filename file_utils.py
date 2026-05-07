import shutil
from pathlib import Path

def ensure_partition_path(base: Path, partition: str) -> Path:
    p = base / f"partition={partition}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def atomic_write(src: Path, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + '.tmp')
    shutil.copy2(src, tmp)
    tmp.replace(dest)

def cleanup_partitions(base: Path, keep_last: int = 7) -> None:
    partitions = sorted(base.glob("partition=*"))
    for old in partitions[:-keep_last]:
        shutil.rmtree(old)