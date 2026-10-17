/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.chaos;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.util.json.JsonObject;

/**
 * One fault rule. A rule is created through {@link ChaosManager#addRule(ChaosRule)} and is active until it expires
 * (after {@link #durationMs} or after {@link #calls} matching calls) or is removed.
 */
public class ChaosRule {

    public enum Type {
        /** Throw an exception instead of calling the endpoint. */
        FAIL,
        /** Wait before calling the endpoint. */
        LATENCY,
        /** Suspend (or stop) the consumer of a route for a while, then resume it. */
        STALL
    }

    // configuration
    private String id;
    private Type type = Type.FAIL;
    private String pattern;
    private String routeId;
    private double probability = 1.0d;
    private long calls;
    private long delayMs;
    private long durationMs;
    private String exception;
    private String message;

    // runtime state (set by the manager)
    private long seq;
    private Random random;
    private long createdAt;
    private long expiresAt;
    private volatile String state = "pending";
    private volatile String endReason;
    private final AtomicLong matched = new AtomicLong();
    private final AtomicLong injected = new AtomicLong();
    private final Map<String, AtomicLong> injectedPerRoute = new ConcurrentHashMap<>();
    private final List<Long> injectedCallNumbers = new ArrayList<>();
    private volatile Map<String, Object> healthBefore;
    private volatile Map<String, Object> healthDuring;
    private volatile Map<String, Object> healthAfter;

    public static ChaosRule fail(String pattern, double probability) {
        ChaosRule r = new ChaosRule();
        r.type = Type.FAIL;
        r.pattern = pattern;
        r.probability = probability;
        return r;
    }

    public static ChaosRule latency(String pattern, long delayMs) {
        ChaosRule r = new ChaosRule();
        r.type = Type.LATENCY;
        r.pattern = pattern;
        r.delayMs = delayMs;
        return r;
    }

    public static ChaosRule stall(String routeId, long durationMs) {
        ChaosRule r = new ChaosRule();
        r.type = Type.STALL;
        r.routeId = routeId;
        r.durationMs = durationMs;
        return r;
    }

    // fluent builder style setters

    public ChaosRule id(String id) {
        this.id = id;
        return this;
    }

    public ChaosRule calls(long calls) {
        this.calls = calls;
        return this;
    }

    public ChaosRule duration(long durationMs) {
        this.durationMs = durationMs;
        return this;
    }

    public ChaosRule delay(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    public ChaosRule probability(double probability) {
        this.probability = probability;
        return this;
    }

    public ChaosRule exception(String exceptionClassName, String message) {
        this.exception = exceptionClassName;
        this.message = message;
        return this;
    }

    // runtime

    void activate(long seq, long seed, long now) {
        this.seq = seq;
        // one random source per rule, derived from the seed and the order the rule was added, so that
        // two rules do not perturb each other and the same seed gives the same faults
        this.random = new Random(seed * 1_000_003L + seq);
        this.createdAt = now;
        this.expiresAt = durationMs > 0 ? now + durationMs : 0;
        this.state = "active";
    }

    boolean isActive() {
        return "active".equals(state);
    }

    boolean isExpiredAt(long now) {
        if (expiresAt > 0 && now >= expiresAt) {
            return true;
        }
        return calls > 0 && matched.get() >= calls;
    }

    void end(String reason) {
        this.state = "ended";
        this.endReason = reason;
    }

    /**
     * Called for each matching call. Returns true when the fault must be injected for this call.
     */
    synchronized boolean roll(String fromRouteId) {
        if (!isActive()) {
            return false;
        }
        if (calls > 0 && matched.get() >= calls) {
            return false;
        }
        long n = matched.incrementAndGet();
        boolean hit = probability >= 1.0d || random.nextDouble() < probability;
        if (hit) {
            injected.incrementAndGet();
            injectedPerRoute.computeIfAbsent(fromRouteId != null ? fromRouteId : "?", k -> new AtomicLong())
                    .incrementAndGet();
            if (injectedCallNumbers.size() < 1000) {
                injectedCallNumbers.add(n);
            }
        }
        return hit;
    }

    synchronized List<Long> getInjectedCallNumbers() {
        return new ArrayList<>(injectedCallNumbers);
    }

    JsonObject toJson() {
        JsonObject jo = new JsonObject();
        jo.put("id", id);
        jo.put("type", type.name().toLowerCase());
        if (pattern != null) {
            jo.put("pattern", pattern);
        }
        if (routeId != null) {
            jo.put("routeId", routeId);
        }
        if (type != Type.STALL) {
            jo.put("probability", probability);
        }
        if (calls > 0) {
            jo.put("calls", calls);
        }
        if (delayMs > 0) {
            jo.put("delayMs", delayMs);
        }
        if (durationMs > 0) {
            jo.put("durationMs", durationMs);
        }
        if (type == Type.FAIL) {
            jo.put("exception", exception != null ? exception : ChaosException.class.getName());
        }
        jo.put("state", state);
        if (endReason != null) {
            jo.put("endReason", endReason);
        }
        jo.put("matched", matched.get());
        jo.put("injected", injected.get());
        Map<String, Object> per = new LinkedHashMap<>();
        injectedPerRoute.forEach((k, v) -> per.put(k, v.get()));
        jo.put("injectedPerRoute", per);
        List<Long> nums = getInjectedCallNumbers();
        if (nums.size() > 50) {
            nums = nums.subList(0, 50);
        }
        jo.put("injectedCallNumbers", nums);
        if (healthBefore != null) {
            jo.put("healthBefore", healthBefore);
        }
        if (healthDuring != null) {
            jo.put("healthDuring", healthDuring);
        }
        if (healthAfter != null) {
            jo.put("healthAfter", healthAfter);
        }
        return jo;
    }

    // getters / setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public long getCalls() {
        return calls;
    }

    public void setCalls(long calls) {
        this.calls = calls;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public String getException() {
        return exception;
    }

    public String getMessage() {
        return message;
    }

    public long getSeq() {
        return seq;
    }

    public long getMatched() {
        return matched.get();
    }

    public long getInjected() {
        return injected.get();
    }

    public String getState() {
        return state;
    }

    public Map<String, Object> getHealthBefore() {
        return healthBefore;
    }

    void setHealthBefore(Map<String, Object> healthBefore) {
        this.healthBefore = healthBefore;
    }

    public Map<String, Object> getHealthDuring() {
        return healthDuring;
    }

    void setHealthDuring(Map<String, Object> healthDuring) {
        this.healthDuring = healthDuring;
    }

    public Map<String, Object> getHealthAfter() {
        return healthAfter;
    }

    void setHealthAfter(Map<String, Object> healthAfter) {
        this.healthAfter = healthAfter;
    }
}
