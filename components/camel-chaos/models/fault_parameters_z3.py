#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
camel-chaos prototype: choose fault parameters with Z3 (pip install z3-solver).

Part 1 (used by ResilienceChaosBreakerTest): the number of consecutive failing calls a chaos `fail` rule needs to
open a resilience4j circuit breaker with a COUNT_BASED window, after a warm-up of successful calls. The breaker
evaluates after each call once the window holds at least minimumNumberOfCalls calls, and opens when
failed * 100 / calls >= failureRateThreshold (resilience4j CircuitBreakerStateMachine.ClosedState).
Z3 finds the smallest N that opens it, and proves that N - 1 failing calls never open it (unsat), for the route's
configuration. The arithmetic is small enough to do by hand; the point is that the same encoding takes the
configuration as input.

Part 2 (model only, no chaos run): the servicebus lock renewer (jira-drafts 962). Runs come 10 or 11 s apart (every
sequence of 6 gaps); a run at time r renews a lock that expires at e when r < e < r + margin. Z3 gives a lock expiry
that no run renews with the 10 s margin (sat, a witness), and proves that with a 20 s margin every expiry in the
horizon is renewed (unsat). An exhaustive count over 100 ms phases is printed next to it.
"""
import itertools
import sys

from z3 import And, Bool, If, Int, Not, Optimize, Or, Solver, Sum, is_true, sat, unsat


def breaker_window(outcomes_fail, window, min_calls, threshold, k):
    """Z3 expression: the breaker opens right after call k (0-based) given fail flags."""
    lo = max(0, k + 1 - window)
    calls = k + 1 - lo
    if calls < min_calls:
        return False
    failed = Sum([If(outcomes_fail[i], 1, 0) for i in range(lo, k + 1)])
    return failed * 100 >= threshold * calls


def part1(warmup=10, window=10, min_calls=10, threshold=50, horizon=30):
    print(f"Part 1: breaker COUNT_BASED window={window} minimumNumberOfCalls={min_calls} "
          f"failureRateThreshold={threshold}% after {warmup} successful calls")
    n = Int("n")
    fail = [Bool(f"f{i}") for i in range(horizon)]
    opt = Optimize()
    opt.add(n >= 1, n <= horizon - warmup)
    for i in range(horizon):
        # warm-up successes, then n consecutive failures, then successes (the rule has ended)
        opt.add(fail[i] == And(i >= warmup, i < warmup + n))
    opens_at = [breaker_window(fail, window, min_calls, threshold, k) for k in range(horizon)]
    opt.add(Or([And(k < warmup + n, o) for k, o in enumerate(opens_at) if not isinstance(o, bool)]))
    opt.minimize(n)
    assert opt.check() == sat
    m = opt.model()
    best = m[n].as_long()
    first_open = next(k for k, o in enumerate(opens_at)
                      if not isinstance(o, bool) and is_true(m.eval(o)))
    print(f"  smallest failing-call count that opens the breaker: N = {best} "
          f"(opens after call {first_open + 1}, the {first_open + 1 - warmup}th failure)")
    # N - 1 failures, in any position after the warm-up, never open it
    s = Solver()
    fail2 = [Bool(f"g{i}") for i in range(horizon)]
    for i in range(warmup):
        s.add(Not(fail2[i]))
    s.add(Sum([If(f, 1, 0) for f in fail2]) <= best - 1)
    s.add(Or([o for o in (breaker_window(fail2, window, min_calls, threshold, k) for k in range(horizon))
              if not isinstance(o, bool)]))
    r = s.check()
    print(f"  {best - 1} failing calls anywhere in {horizon} calls open it: {r} "
          f"({'proved: never opens' if r == unsat else 'counterexample'})")
    return best


def renewed(runs, e, margin):
    return any(r < e < r + margin for r in runs)


def part2():
    print("Part 2: servicebus lock renewer (times in 100 ms units), runs 10 or 11 s apart, 6 gaps")
    for margin in (100, 200):
        g = [Int(f"g{i}") for i in range(6)]
        e = Int("e")
        runs = [Sum(g[:i]) if i else 0 for i in range(7)]
        s = Solver()
        s.add([Or(x == 100, x == 110) for x in g])
        s.add(e > 0, e <= runs[6])
        s.add(And([Not(And(r < e, e < r + margin)) for r in runs]))
        res = s.check()
        if res == sat:
            m = s.model()
            gaps = [m[x].as_long() for x in g]
            print(f"  margin {margin / 10:.0f} s: sat, lock lost, witness gaps={[x / 10 for x in gaps]} s, "
                  f"expiry e={m[e].as_long() / 10} s")
        else:
            print(f"  margin {margin / 10:.0f} s: {res} (proved: every expiry in the horizon is renewed)")
        # exhaustive count (labelled as such)
        lost = total = 0
        for gaps in itertools.product((100, 110), repeat=6):
            runs_c = [sum(gaps[:i]) for i in range(7)]
            for ev in range(1, runs_c[6] + 1):
                total += 1
                lost += not renewed(runs_c, ev, margin)
        print(f"  margin {margin / 10:.0f} s: exhaustive count, lock lost in {lost} of {total} cases "
              f"({100.0 * lost / total:.1f} %)")


if __name__ == "__main__":
    n = part1()
    part2()
    sys.exit(0 if n else 1)
