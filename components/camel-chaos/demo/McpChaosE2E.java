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

import java.lang.reflect.Field;

import org.apache.camel.dsl.jbang.core.commands.mcp.RuntimeService;
import org.apache.camel.dsl.jbang.core.commands.mcp.RuntimeTools;
import org.apache.camel.util.json.JsonObject;

/**
 * Calls the MCP tool camel_runtime_chaos of camel-jbang-mcp, as an MCP client would, against the running demo
 * application (chaos-demo): the tool method is invoked directly (no MCP transport), then the fault happens in the
 * application and the report comes back through the tool. Run with mcp-e2e.sh.
 */
public class McpChaosE2E {

    public static void main(String[] args) throws Exception {
        RuntimeTools tools = new RuntimeTools();
        Field f = RuntimeTools.class.getDeclaredField("runtimeService");
        f.setAccessible(true);
        f.set(tools, new RuntimeService());

        System.out.println("camel_runtime_processes -> " + tools.camel_runtime_processes());

        // the backend is down for 3 calls: with 2 redeliveries one message goes to the dead letter channel
        print("add", tools.camel_runtime_chaos("chaos-demo", "add", "fail", "log:backend*", null, "100%", "3", null, null,
                "java.io.IOException", "mcp-backend-down", null, null));
        // and the consumer must still deliver 20 more messages
        print("expect", tools.camel_runtime_chaos("chaos-demo", "expect", null, null, "orders", null, null, null, null,
                null, null, null, "20"));
        Thread.sleep(3500);
        JsonObject report = tools.camel_runtime_chaos("chaos-demo", null, null, null, null, null, null, null, null, null,
                null, null, null);
        print("report", report);
        JsonObject r = (JsonObject) report.get("report");
        System.out.println("RESULT=" + r.get("result"));
    }

    private static void print(String what, JsonObject jo) {
        System.out.println("camel_runtime_chaos " + what + " -> " + jo.toJson());
    }
}
