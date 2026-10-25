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
package org.apache.camel.component.milo.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.milo.AbstractMiloServerTest;
import org.apache.camel.component.mock.MockEndpoint;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.junit.jupiter.api.Test;

import static org.apache.camel.component.milo.NodeIds.nodeValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The item of a milo-server endpoint stays in the Items folder of the server while a route still uses it, and comes
 * back when the route that consumes from it is added again.
 */
class ServerRemoveRouteTest extends AbstractMiloServerTest {

    private static final String ITEM = "milo-server:removeRouteItem";
    private static final String BROWSE_ITEMS = "milo-browse:opc.tcp://foo:bar@127.0.0.1:@@port@@"
                                               + "?overrideHost=true&allowedSecurityPolicies=None&node="
                                               + nodeValue(MiloServerComponent.DEFAULT_NAMESPACE_URI, "items");
    private static final String CLIENT_ITEM = "milo-client:opc.tcp://foo:bar@127.0.0.1:@@port@@"
                                              + "?overrideHost=true&allowedSecurityPolicies=None&node="
                                              + nodeValue(MiloServerComponent.DEFAULT_NAMESPACE_URI, "removeRouteItem");

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // OPC UA clients write the item (consumer) and the route publishes its value (producer)
                from(ITEM).routeId("writes").to("mock:writes");
                from("direct:update").routeId("update").to(ITEM);

                from("direct:browse").enrich(resolve(BROWSE_ITEMS));

                // another endpoint of the same item, and an OPC UA client that writes the item
                from(ITEM + "?bridgeErrorHandler=true").routeId("otherWrites").autoStartup(false)
                        .to("mock:otherWrites");
                from("direct:write").routeId("clientWrite").autoStartup(false).to(resolve(CLIENT_ITEM));
            }
        };
    }

    @Test
    void removeConsumerRouteKeepsTheItemOfTheProducer() throws Exception {
        assertTrue(browseItems().contains("removeRouteItem"), "browse: " + browseItems());

        context.getRouteController().stopRoute("writes");
        context.removeRoute("writes");
        template.sendBody("direct:update", 42);

        assertTrue(browseItems().contains("removeRouteItem"), "the producer route still publishes the item");
    }

    @Test
    void removeConsumerRouteThenAddItAgain() throws Exception {
        context.getRouteController().stopRoute("writes");
        context.removeRoute("writes");
        context.getRouteController().stopRoute("update");
        context.removeRoute("update");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from(ITEM).routeId("writes2").to("mock:writes2");
            }
        });

        assertTrue(browseItems().contains("removeRouteItem"), "the route was added again: " + browseItems());
        MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void removeConsumerRouteKeepsTheWritesToAnotherEndpointOfTheItem() throws Exception {
        context.getRouteController().startRoute("otherWrites");
        context.getRouteController().startRoute("clientWrite");

        context.getRouteController().stopRoute("writes");
        context.removeRoute("writes");

        // the other endpoint still listens to the writes of the item
        MockEndpoint otherWrites = getMockEndpoint("mock:otherWrites");
        otherWrites.expectedMinimumMessageCount(1);
        template.sendBody("direct:write", 7);
        otherWrites.assertIsSatisfied();
    }

    @SuppressWarnings("unchecked")
    private List<String> browseItems() {
        Map<ExpandedNodeId, BrowseResult> results = template.requestBody("direct:browse", null, Map.class);
        List<String> names = new ArrayList<>();
        for (BrowseResult result : results.values()) {
            if (result.getReferences() != null) {
                for (ReferenceDescription reference : result.getReferences()) {
                    names.add(reference.getDisplayName().getText());
                }
            }
        }
        return names;
    }
}
