#!/usr/bin/env python3
import json, os, pathlib, sys, time
root = pathlib.Path(sys.argv[1])
scenario = json.loads((root / 'scenario.json').read_text())
action = json.loads(sys.stdin.readline())
with (root / 'handler-events.jsonl').open('a') as stream:
    stream.write(json.dumps({'argv': sys.argv[2:], 'pid': os.getpid(), 'action': action}) + '\n')
if scenario.get('hold'):
    while not (root / 'release').exists():
        time.sleep(0.01)
time.sleep(scenario.get('delay_ms', 0) / 1000)
if scenario.get('stderr_flood'):
    sys.stderr.write('x' * 1048576)
mode = scenario.get('mode', 'success')
if mode == 'malformed':
    print('{bad')
elif mode == 'pretty':
    print(json.dumps({'schemaVersion':1,'ok':True,'outcome':'opened'}, indent=2))
elif mode == 'unterminated':
    sys.stdout.write(json.dumps({'schemaVersion':1,'ok':True,'outcome':'opened'}))
elif mode == 'multiple':
    print('{"schemaVersion":1,"ok":true,"outcome":"opened"}\n{}')
elif mode == 'oversized':
    print('x' * 8192)
elif mode == 'failure':
    print(json.dumps({'schemaVersion':1,'ok':False,'error':'cdp_unavailable'}))
    sys.exit(1)
else:
    print(json.dumps({'schemaVersion':1,'ok':True,'outcome':'opened'}), flush=True)
sys.exit(scenario.get('exit_code', 0))
