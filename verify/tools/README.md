# Tools

`story.py` turns a TLC counterexample into a short numbered story: the action of each step, in plain words, and the
variables it changed.

```
python3 story.py <tlc-output.txt> [narration.json]
```

The narration file maps each action of the model to one line (`{arg}` is replaced by the action's argument) and can
hide bookkeeping variables with `"_hide"`. See `../models/narr_R1.json`. `../models/run_all.sh` calls it for every
violation.
