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

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proof point (b): reproducibility. Three runs with the same seed give a byte-identical canonical report (same
 * SHA-256); another seed gives another fault schedule and another report. Each run is a fresh CamelContext.
 */
class ChaosReproducibilityTest {

    private static final String P = "b-reproducibility";

    @BeforeAll
    static void title() {
        Proof.start(P, "Proof point (b): reproducibility (ChaosReproducibilityTest): 300 messages, fail 25% of"
                       + " mock:backend, latency 1ms on 10% of mock:audit, 1 redelivery + dead letter channel");
    }

    @Test
    void sameSeedSameReportOtherSeedOtherSchedule() throws Exception {
        List<Run> runs = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Run r = run(11);
            runs.add(r);
            Proof.log(P, "seed 11 run " + i + ": sha256=" + r.sha + " backend faults at calls " + head(r.failCalls));
            Files.writeString(Proof.dir().resolve("b-report-seed11-run" + i + ".json"), r.canonical);
        }
        Run other = run(12);
        Proof.log(P, "seed 12 run 1: sha256=" + other.sha + " backend faults at calls " + head(other.failCalls));
        Files.writeString(Proof.dir().resolve("b-report-seed12-run1.json"), other.canonical);

        assertThat(runs.get(1).canonical).isEqualTo(runs.get(0).canonical);
        assertThat(runs.get(2).canonical).isEqualTo(runs.get(0).canonical);
        assertThat(runs.get(1).sha).isEqualTo(runs.get(0).sha);
        assertThat(runs.get(2).sha).isEqualTo(runs.get(0).sha);
        assertThat(other.failCalls).isNotEqualTo(runs.get(0).failCalls);
        assertThat(other.sha).isNotEqualTo(runs.get(0).sha);
        Proof.log(P, "RESULT: 3/3 identical for seed 11; seed 12 differs (schedule and SHA-256)");
    }

    private static String head(List<Long> calls) {
        return calls.subList(0, Math.min(12, calls.size())) + (calls.size() > 12 ? " ... (" + calls.size() + ")" : "");
    }

    private static Run run(long seed) throws Exception {
        CamelContext context = ChaosManagerTest.newContext(seed, true, new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(deadLetterChannel("mock:dead").maximumRedeliveries(1).redeliveryDelay(0));
                from("direct:orders").routeId("orders").to("mock:audit").to("mock:backend");
            }
        });
        try {
            ChaosManager manager = ChaosManager.get(context);
            ChaosRule fail = manager.addRule(ChaosRule.fail("mock:backend", 0.25).id("backend"));
            manager.addRule(ChaosRule.latency("mock:audit", 1).probability(0.1).id("audit-slow"));
            ProducerTemplate template = context.createProducerTemplate();
            for (int i = 0; i < 300; i++) {
                try {
                    template.sendBody("direct:orders", "order-" + i);
                } catch (CamelExecutionException e) {
                    // counted in the report
                }
            }
            manager.clear();
            template.sendBody("direct:orders", "after");
            return new Run(manager.canonicalReport(), manager.canonicalReportSha256(), fail.getInjectedCallNumbers());
        } finally {
            context.stop();
        }
    }

    private record Run(String canonical, String sha, List<Long> failCalls) {
    }
}
