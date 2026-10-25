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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.annotations.DevConsole;
import org.apache.camel.support.console.AbstractDevConsole;
import org.apache.camel.util.json.JsonObject;

/**
 * Dev console to drive chaos rules and read the report. Camel CLI and the MCP server reach it through the CLI connector
 * ({@code action=chaos}); over HTTP it is {@code /q/dev/chaos?action=...}.
 */
@DevConsole(name = "chaos", displayName = "Chaos",
            description = "Inject faults (dev/test only) and report how the routes coped",
            readOnly = false)
public class ChaosDevConsole extends AbstractDevConsole {

    @Metadata(label = "query", description = "What to do: report (default), add, remove, clear, reset, expect, invariant",
              defaultValue = "report")
    public static final String ACTION = "action";
    @Metadata(label = "query", description = "Rule type for add: fail, latency, stall", defaultValue = "fail")
    public static final String TYPE = "type";
    @Metadata(label = "query", description = "Endpoint pattern for fail and latency rules (wildcard or regex, as in intercept)")
    public static final String PATTERN = "pattern";
    @Metadata(label = "query", description = "Route id for a stall rule")
    public static final String ROUTE_ID = "routeId";
    @Metadata(label = "query", description = "Probability (0..1) that a matching call gets the fault",
              javaType = "java.lang.Double",
              defaultValue = "1.0")
    public static final String PROBABILITY = "probability";
    @Metadata(label = "query", description = "The rule applies to the next N matching calls, then ends (0 = no limit)",
              javaType = "java.lang.Long")
    public static final String CALLS = "calls";
    @Metadata(label = "query", description = "Latency to add in millis (latency rule)", javaType = "java.lang.Long")
    public static final String DELAY = "delay";
    @Metadata(label = "query", description = "Rule lifetime in millis (0 = until removed or calls reached)",
              javaType = "java.lang.Long")
    public static final String DURATION = "duration";
    @Metadata(label = "query", description = "Exception class to throw (fail rule); needs a String constructor")
    public static final String EXCEPTION = "exception";
    @Metadata(label = "query", description = "Exception message (fail rule)")
    public static final String MESSAGE = "message";
    @Metadata(label = "query", description = "Stall rule with a pattern: the matching call that suspends the route",
              javaType = "java.lang.Long", defaultValue = "1")
    public static final String AT_CALL = "atCall";
    @Metadata(label = "query", description = "Expect: the number of exchanges the route must still complete",
              javaType = "java.lang.Long")
    public static final String COUNT = "count";
    @Metadata(label = "query",
              description = "Invariant: a predicate in the simple language every exchange of routeId must match when it completes")
    public static final String PREDICATE = "predicate";
    @Metadata(label = "query", description = "Rule id (add: optional, remove: required)")
    public static final String ID = "id";

    public ChaosDevConsole() {
        super("camel", "chaos", "Chaos", "Inject faults (dev/test only) and report how the routes coped");
    }

    @Override
    protected String doCallText(Map<String, Object> options) {
        ChaosManager manager = ChaosManager.get(getCamelContext());
        if (manager == null) {
            return "camel-chaos is not installed";
        }
        Map<String, Object> result = apply(manager, options);
        StringBuilder sb = new StringBuilder();
        if (result.get("result") != null) {
            sb.append(result.get("result")).append(System.lineSeparator());
        }
        if (result.get("error") != null) {
            sb.append("ERROR: ").append(result.get("error")).append(System.lineSeparator());
        }
        sb.append(manager.reportText());
        return sb.toString();
    }

    @Override
    protected Map<String, Object> doCallJson(Map<String, Object> options) {
        ChaosManager manager = ChaosManager.get(getCamelContext());
        JsonObject root = new JsonObject();
        if (manager == null) {
            root.put("error", "camel-chaos is not installed");
            return root;
        }
        Map<String, Object> result = apply(manager, options);
        root.putAll(result);
        root.put("report", manager.report());
        return root;
    }

    private Map<String, Object> apply(ChaosManager manager, Map<String, Object> options) {
        Map<String, Object> answer = new LinkedHashMap<>();
        String action = optionString(options, ACTION);
        if (action == null) {
            action = "report";
        }
        answer.put("action", action);
        try {
            switch (action) {
                case "add" -> {
                    ChaosRule rule = createRule(options);
                    manager.addRule(rule);
                    answer.put("result", "added rule " + rule.getId());
                    answer.put("rule", rule.getId());
                }
                case "remove" -> {
                    String id = optionString(options, ID);
                    boolean removed = manager.removeRule(id);
                    answer.put("result", removed ? "removed rule " + id : "no active rule " + id);
                }
                case "clear" -> {
                    manager.clear();
                    answer.put("result", "ended all active rules");
                }
                case "reset" -> {
                    manager.reset();
                    answer.put("result", "cleared rules, statistics, expectations and timeline; rule order restarted");
                }
                case "expect" -> {
                    String routeId = optionString(options, ROUTE_ID);
                    long count = optionLong(options, COUNT, 0);
                    manager.expect(routeId, count);
                    answer.put("result", "expecting " + count + " more exchanges on route " + routeId);
                }
                case "invariant" -> {
                    String routeId = optionString(options, ROUTE_ID);
                    String predicate = optionString(options, PREDICATE);
                    ChaosManager.Invariant inv = manager.invariant(routeId, optionString(options, ID), predicate);
                    answer.put("result", "checking invariant on route " + routeId + ": " + predicate);
                    answer.put("invariant", inv.toJson().get("name"));
                }
                case "report" -> {
                    // report only
                }
                default -> answer.put("error", "unknown action " + action);
            }
        } catch (RuntimeException e) {
            answer.put("error", e.getMessage());
        }
        return answer;
    }

    private ChaosRule createRule(Map<String, Object> options) {
        String type = optionString(options, TYPE);
        ChaosRule rule = new ChaosRule();
        rule.setType(type == null ? ChaosRule.Type.FAIL : ChaosRule.Type.valueOf(type.toUpperCase()));
        rule.setId(optionString(options, ID));
        rule.setPattern(optionString(options, PATTERN));
        rule.setRouteId(optionString(options, ROUTE_ID));
        String p = optionString(options, PROBABILITY);
        if (p != null) {
            double d = Double.parseDouble(p.trim().replace("%", ""));
            rule.setProbability(p.contains("%") || d > 1 ? d / 100d : d);
        }
        rule.setCalls(optionLong(options, CALLS, 0));
        rule.setDelayMs(optionLong(options, DELAY, 0));
        rule.setDurationMs(optionLong(options, DURATION, 0));
        rule.setAtCall(optionLong(options, AT_CALL, 1));
        rule.exception(optionString(options, EXCEPTION), optionString(options, MESSAGE));
        return rule;
    }
}
