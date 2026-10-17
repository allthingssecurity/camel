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
package org.apache.camel.dsl.jbang.core.commands.action;

import java.util.List;
import java.util.Map;

import org.apache.camel.dsl.jbang.core.commands.CamelJBangMain;
import org.apache.camel.dsl.jbang.core.common.JsonOutput;
import org.apache.camel.dsl.jbang.core.common.ProcessFinder.CamelProcess;
import org.apache.camel.util.json.JsonObject;
import picocli.CommandLine;

@CommandLine.Command(name = "chaos",
                     description = "Inject faults into a running Camel integration and report how it coped (prototype, dev/test only, needs camel-chaos)",
                     sortOptions = false, showDefaultValues = true,
                     footer = {
                             "%nExamples:",
                             "  camel cmd chaos --command=add --type=fail --pattern=http* --probability=30% --calls=20",
                             "  camel cmd chaos --command=add --type=latency --pattern=kafka:* --delay=2000 --duration=60000",
                             "  camel cmd chaos --command=add --type=stall --route-id=orders --duration=5000",
                             "  camel cmd chaos                       (prints the report)",
                             "  camel cmd chaos --command=clear" })
public class CamelChaosAction extends ActionBaseCommand {

    @CommandLine.Parameters(description = "Name or pid of running Camel integration", arity = "0..1")
    String name = "*";

    @CommandLine.Option(names = { "--command" }, description = "report, add, remove, clear or reset",
                        defaultValue = "report")
    String command = "report";

    @CommandLine.Option(names = { "--type" }, description = "Rule type for add: fail, latency or stall")
    String type;

    @CommandLine.Option(names = { "--pattern" }, description = "Endpoint pattern for fail and latency rules")
    String pattern;

    @CommandLine.Option(names = { "--route-id" }, description = "Route id for a stall rule")
    String routeId;

    @CommandLine.Option(names = { "--probability" }, description = "0..1 or a percentage such as 30%")
    String probability;

    @CommandLine.Option(names = { "--calls" }, description = "The rule applies to the next N matching calls")
    String calls;

    @CommandLine.Option(names = { "--delay" }, description = "Latency in millis (latency rule)")
    String delay;

    @CommandLine.Option(names = { "--duration" }, description = "Rule lifetime in millis")
    String duration;

    @CommandLine.Option(names = { "--exception" }, description = "Exception class to throw (fail rule)")
    String exception;

    @CommandLine.Option(names = { "--id" }, description = "Rule id (add: optional, remove: required)")
    String id;

    @CommandLine.Option(names = { "--json" }, description = "Output the full report as JSON")
    boolean json;

    @CommandLine.Mixin
    ActionTimeoutMixin timeoutOption;

    public CamelChaosAction(CamelJBangMain main) {
        super(main);
    }

    @Override
    public Integer doCall() throws Exception {
        CamelProcess process = findOne(name);
        JsonObject answer = executeAction(process, "chaos", root -> {
            root.put("command", command);
            putIfSet(root, "type", type);
            putIfSet(root, "pattern", pattern);
            putIfSet(root, "routeId", routeId);
            putIfSet(root, "probability", probability);
            putIfSet(root, "calls", calls);
            putIfSet(root, "delay", delay);
            putIfSet(root, "duration", duration);
            putIfSet(root, "exception", exception);
            putIfSet(root, "id", id);
        });
        if (json) {
            JsonOutput.print(printer(), answer);
            return 0;
        }
        if (answer.getString("result") != null) {
            printer().println(answer.getString("result"));
        }
        JsonObject report = answer.get("report") instanceof JsonObject jo ? jo : new JsonObject();
        printer().println("Chaos enabled=" + report.get("enabled") + " seed=" + report.get("seed"));
        if (report.get("rules") instanceof List<?> rules) {
            for (Object o : rules) {
                if (o instanceof Map<?, ?> r) {
                    printer().println(String.format("  rule %-14s %-7s %-20s %-6s matched=%s injected=%s health %s/%s/%s",
                            r.get("id"), r.get("type"), r.get("pattern") != null ? r.get("pattern") : r.get("routeId"),
                            r.get("state"), r.get("matched"), r.get("injected"), health(r.get("healthBefore")),
                            health(r.get("healthDuring")), health(r.get("healthAfter"))));
                }
            }
        }
        if (report.get("routes") instanceof Map<?, ?> routes) {
            for (Map.Entry<?, ?> e : routes.entrySet()) {
                if (e.getValue() instanceof Map<?, ?> r) {
                    printer().println(String.format(
                            "  route %-14s completed=%s failed=%s errorHandled=%s redeliveries=%s faults=%s -> %s",
                            e.getKey(), r.get("completed"), r.get("failed"), r.get("failureHandled"), r.get("redeliveries"),
                            r.get("faultsInjected"), r.get("verdict")));
                }
            }
        }
        return 0;
    }

    private static Object health(Object h) {
        return h instanceof Map<?, ?> m ? m.get("state") : "-";
    }

    private static void putIfSet(JsonObject root, String key, String value) {
        if (value != null && !value.isBlank()) {
            root.put(key, value);
        }
    }
}
