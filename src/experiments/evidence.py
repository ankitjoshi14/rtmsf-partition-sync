"""Save experiment inputs, source identity and immutable evidence files."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class EvidenceBundle:
    def __init__(self, root: Path, manifest: dict):
        self.root = root
        root.mkdir(parents=True, exist_ok=False)
        self.write("manifest.json", manifest)

    def write_bytes(self, name: str, data: bytes) -> None:
        path = self.root / name
        if not path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError("evidence path escapes bundle")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(data)

    def write(self, name: str, data) -> None:
        self.write_bytes(name, (json.dumps(data, indent=2, sort_keys=True, default=str) + "\n").encode())


def source_snapshot(root: Path) -> dict:
    if not (root / "pyproject.toml").is_file() or not (root / "src/rtmsf").is_dir():
        raise ValueError("run experiments from the repository root to retain their source")
    files = []
    for name in ("src", "tests", "docs"):
        files.extend(path for path in (root / name).rglob("*") if path.is_file() and path.suffix in {".py", ".yaml", ".yml", ".sh", ".md", ".csv"})
    files.extend(root / name for name in ("pyproject.toml", "uv.lock", "Dockerfile", ".dockerignore", "README.md", "LICENSE", "CITATION.cff", "MANIFEST.in") if (root / name).is_file())
    hashes = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)}
    return {"sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(), "files": hashes}
