#!/usr/bin/env python3
"""Turn a TLC counterexample into a short step-by-step story: action + what changed.
usage: story.py <tlc-output.txt> [narration.json]"""
import json, re, sys

def parse(path):
    states, cur = [], None
    for line in open(path):
        m = re.match(r'State (\d+): <(\w+)(\([^)]*\))?', line)
        if m:
            cur = {'action': m.group(2), 'args': (m.group(3) or '').strip('()'), 'vars': {}}
            states.append(cur); continue
        m = re.match(r'/\\ (\w+) = (.*)', line.strip())
        if m and cur is not None:
            cur['vars'][m.group(1)] = m.group(2)
    return states

def main():
    states = parse(sys.argv[1])
    narr = json.load(open(sys.argv[2])) if len(sys.argv) > 2 else {}
    hide = set(narr.get('_hide', []))
    if not states:
        print('no counterexample'); return
    print('start: ' + ', '.join(f'{k}={v}' for k, v in states[0]['vars'].items() if k not in hide))
    for i in range(1, len(states)):
        s, p = states[i], states[i - 1]
        what = narr.get(s['action'], s['action']).replace('{arg}', s['args'])
        diff = [f'{k}: {p["vars"].get(k)} -> {v}' for k, v in s['vars'].items()
                if p['vars'].get(k) != v and k not in hide]
        print(f'{i}. {what}' + (f'   [{"; ".join(diff)}]' if diff else ''))

main()
