#!/usr/bin/env python3
"""Fill template.html with the spec and write index.html (the same step as kit/anim.py `prepare` in
claude-animation-skills: the spec carries the words, the template the shapes and the timeline).

    python3 build.py [verify-explainer.json]
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec_path = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / 'verify-explainer.json'
spec = json.loads(spec_path.read_text())
html = (HERE / 'template.html').read_text()
data = json.dumps(spec, ensure_ascii=False).replace('</', '<\\/')
out = html.replace('__SPEC__', data).replace('__DURATION__', f"{float(spec['duration']):.3f}")
(HERE / 'index.html').write_text(out)
print('wrote', HERE / 'index.html')
