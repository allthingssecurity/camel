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
import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proof point (c): overhead when off. A simple timing harness (not JMH): the same route in five set-ups, measured in
 * interleaved rounds (the order rotates every round) after a warm-up, reported as the median and the spread of the
 * per-round mean. A second baseline (A/A) shows the noise between two identical set-ups. Run with
 * {@code -Dchaos.bench=true}; it is skipped in a normal build.
 * <ul>
 * <li>baseline: camel-chaos removed from the context (as if the jar were absent)</li>
 * <li>off: camel-chaos on the classpath, not enabled (the default): the hooks are taken out when the context
 * starts</li>
 * <li>enabled-idle: enabled, no rule</li>
 * <li>rule-elsewhere: enabled, one rule on another endpoint (pattern does not match)</li>
 * <li>rule-ended: enabled, a rule was added and removed (the statistics notifier is now on)</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "chaos.bench", matches = "true")
class ChaosOverheadTest {

    private static final String P = "c-overhead";
    private static final int PER_ROUND = Integer.getInteger("chaos.bench.perRound", 200_000);
    private static final int ROUNDS = Integer.getInteger("chaos.bench.rounds", 15);
    private static final int WARMUP = Integer.getInteger("chaos.bench.warmup", 5);

    @Test
    void overheadWhenOff() throws Exception {
        Proof.start(P, "Proof point (c): overhead (ChaosOverheadTest), route direct:a -> to(direct:b) -> setBody, "
                       + PER_ROUND + " exchanges per round, " + WARMUP + " warm-up + " + ROUNDS
                       + " measured rounds, interleaved and rotated, JDK " + System.getProperty("java.version") + ", "
                       + Runtime.getRuntime().availableProcessors() + " CPUs");
        Map<String, CamelContext> setups = new LinkedHashMap<>();
        setups.put("baseline", context(Mode.BASELINE));
        setups.put("off", context(Mode.OFF));
        setups.put("enabled-idle", context(Mode.ENABLED_IDLE));
        setups.put("rule-elsewhere", context(Mode.RULE_ELSEWHERE));
        setups.put("rule-ended", context(Mode.RULE_ENDED));
        // a second, identical baseline: the A/A control, it shows how far two equal set-ups drift apart
        setups.put("baseline-A/A", context(Mode.BASELINE));
        Map<String, List<Double>> results = new LinkedHashMap<>();
        setups.keySet().forEach(k -> results.put(k, new ArrayList<>()));
        try {
            List<String> order = new ArrayList<>(setups.keySet());
            for (int round = 0; round < WARMUP + ROUNDS; round++) {
                // rotate the order every round, so no set-up always runs first or right after another
                for (int k = 0; k < order.size(); k++) {
                    String key = order.get((k + round) % order.size());
                    double ns = measure(setups.get(key));
                    if (round >= WARMUP) {
                        results.get(key).add(ns);
                    }
                }
            }
        } finally {
            setups.values().forEach(CamelContext::stop);
        }
        double base = median(results.get("baseline"));
        for (Map.Entry<String, List<Double>> e : results.entrySet()) {
            double[] v = e.getValue().stream().mapToDouble(Double::doubleValue).sorted().toArray();
            double med = median(e.getValue());
            Proof.log(P, String.format("%-15s median %7.1f ns/exchange  (min %7.1f, max %7.1f)  vs baseline %+6.1f%%",
                    e.getKey(), med, v[0], v[v.length - 1], (med - base) * 100 / base));
        }
        // the noise between rounds of the same set-up, for comparison
        double[] b = results.get("baseline").stream().mapToDouble(Double::doubleValue).sorted().toArray();
        Proof.log(P, String.format("noise: baseline rounds spread %.1f%% (max-min over median)",
                (b[b.length - 1] - b[0]) * 100 / base));
        assertThat(base).isPositive();
    }

    private static double measure(CamelContext context) {
        ProducerTemplate template = context.createProducerTemplate();
        long start = System.nanoTime();
        for (int i = 0; i < PER_ROUND; i++) {
            template.sendBody("direct:a", "x");
        }
        long took = System.nanoTime() - start;
        return (double) took / PER_ROUND;
    }

    private static double median(List<Double> values) {
        double[] v = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        return v.length % 2 == 1 ? v[v.length / 2] : (v[v.length / 2 - 1] + v[v.length / 2]) / 2;
    }

    enum Mode {
        BASELINE,
        OFF,
        ENABLED_IDLE,
        RULE_ELSEWHERE,
        RULE_ENDED
    }

    private static CamelContext context(Mode mode) throws Exception {
        CamelContext context = new DefaultCamelContext();
        if (mode == Mode.BASELINE) {
            ChaosManager.uninstall(context);
        }
        if (mode == Mode.ENABLED_IDLE || mode == Mode.RULE_ELSEWHERE || mode == Mode.RULE_ENDED) {
            Properties props = new Properties();
            props.put(ChaosManager.ENABLED, "true");
            context.getPropertiesComponent().setOverrideProperties(props);
        }
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:a").to("direct:b");
                from("direct:b").setBody(constant("y"));
            }
        });
        context.start();
        ChaosManager manager = ChaosManager.get(context);
        assertThat(manager.isEnabled()).isEqualTo(mode != Mode.BASELINE && mode != Mode.OFF);
        if (mode == Mode.RULE_ELSEWHERE) {
            manager.addRule(ChaosRule.fail("mock:not-used", 1.0));
        }
        if (mode == Mode.RULE_ENDED) {
            manager.addRule(ChaosRule.fail("mock:not-used", 1.0).id("x"));
            manager.removeRule("x");
        }
        return context;
    }
}
