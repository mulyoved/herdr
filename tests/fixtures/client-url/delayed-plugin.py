import os, pathlib, subprocess, sys, time
root = pathlib.Path(sys.argv[1])
(root / 'origin.txt').write_text(os.environ.get('HERDR_ORIGIN_CLIENT_ID', ''))
(root / 'context.json').write_text(os.environ.get('HERDR_PLUGIN_CONTEXT_JSON', '{}'))
while not (root / 'release').exists():
    time.sleep(0.01)
result = subprocess.run([os.environ['HERDR_BIN_PATH'], 'client', 'open-url',
                         'https://example.com/', '--json'], capture_output=True, text=True)
(root / 'result.json').write_text(result.stdout)
sys.exit(result.returncode)
