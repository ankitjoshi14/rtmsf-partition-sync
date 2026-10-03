"""Save experiment inputs, source identity and immutable evidence files."""

from datetime import datetime, timezone
import hashlib
import json
import re
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


def validate_image_manifest(build: dict, source_files: dict) -> None:
    """Reject stale runtime/build inputs before an experiment changes AWS resources."""
    image = build.get('image')
    if not isinstance(image, str) or re.fullmatch(r'.+@sha256:[0-9a-f]{64}', image) is None:
        raise ValueError('build manifest needs an immutable image digest; rebuild using docs/aws.md')
    runtime = {name: value for name, value in source_files.items()
               if name.startswith('src/rtmsf/') or name in {
                   'src/experiments/__init__.py', 'src/experiments/aws/__init__.py',
                   'src/experiments/aws/crash_worker.py', 'src/experiments/aws/activity_worker.py'}}
    if build.get('files') != runtime:
        raise ValueError('runtime files differ from the recorded worker image')
    names = ('Dockerfile', 'pyproject.toml', '.dockerignore')
    if any(name not in source_files for name in names) or build.get('build_files') != {name: source_files[name] for name in names}:
        raise ValueError('image build inputs differ or are missing; rebuild using docs/aws.md')
