"""Capture a cloud witness without truncation by Portal's output limit."""
import base64
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

stage = sys.argv[1]
assert stage in {'baseline', 'first', 'second', 'overwrite', 'partial-delete',
    'last-delete', 'burst', 'replay', 'recovery', 'recovery-queued',
    'recovery-delete', 'recovery-delete-pending', 'cleanup', 'dlq'}
command = ['/opt/rtmsf/current/.venv/bin/python', '/opt/rtmsf/live-witness.py', stage]
if stage == 'dlq':
    command = command[:1] + ['/opt/rtmsf/dlq-witness.py']
result = subprocess.run(command, timeout=150, capture_output=True)
document = json.loads(result.stdout)
root = Path('/var/lib/rtmsf/proof-evidence')
root.mkdir(mode=0o700, exist_ok=True)
output = root / (stage + '-' + str(time.time_ns()) + '.json')
output.write_bytes(result.stdout)
encoded = base64.b64encode(gzip.compress(result.stdout)).decode()
assert len(encoded) < 3000, 'proof_output_requires_chunked_export'
print(json.dumps({'stage': stage, 'result': document.get('result', 'OBSERVED'),
    'return_code': result.returncode, 'rows': len(document.get('rows', [])),
    'partitions': document.get('partitions'), 'pending_queue': len(document.get('queue_peek', [])),
    'dlq_messages': len(document.get('dlq_peek', [])),
    'sha256': hashlib.sha256(result.stdout).hexdigest(), 'gzip_base64': encoded}), flush=True)
sys.exit(result.returncode)
