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
package org.apache.camel.coap;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A route serves only the resource of its own path, and only while it is started.
 */
class CoAPConsumerStopTest extends CoAPTestSupport {

    @Test
    void aParentPathOfARouteIsNotServedByThatRoute() throws Exception {
        assertResponse("/b/c", ResponseCode.CONTENT, "reply-bc");
        assertResponse("/b", ResponseCode.NOT_FOUND, "");
    }

    @Test
    void aParentPathOfARestRouteIsNotServedByThatRoute() throws Exception {
        assertResponse("/orders/1", ResponseCode.CONTENT, "order 1");
        assertResponse("/orders", ResponseCode.NOT_FOUND, "");
    }

    @Test
    void aStoppedRouteNoLongerServesItsResource() throws Exception {
        assertResponse("/a", ResponseCode.CONTENT, "reply-a");

        context.getRouteController().stopRoute("a");
        assertResponse("/a", ResponseCode.NOT_FOUND, "");

        context.getRouteController().startRoute("a");
        assertResponse("/a", ResponseCode.CONTENT, "reply-a");
    }

    @Test
    void aSuspendedRouteNoLongerServesItsResource() throws Exception {
        context.getRouteController().suspendRoute("a");
        assertResponse("/a", ResponseCode.NOT_FOUND, "");

        context.getRouteController().resumeRoute("a");
        assertResponse("/a", ResponseCode.CONTENT, "reply-a");
    }

    @Test
    void aRemovedRouteNoLongerServesItsMethodOfAResource() throws Exception {
        context.getRouteController().stopRoute("dPost");
        context.removeRoute("dPost");

        // the other route of the path still serves its method
        CoapClient client = createClient("/d");
        try {
            CoapResponse response = client.get();
            assertNotNull(response);
            assertEquals(ResponseCode.CONTENT, response.getCode());
            assertEquals("reply-dGet", response.getResponseText());
        } finally {
            client.shutdown();
        }
        assertResponse("/d", ResponseCode.METHOD_NOT_ALLOWED, "");
    }

    @Test
    void aRemovedRouteThatIsAddedAgainServesItsResource() throws Exception {
        context.getRouteController().stopRoute("a");
        context.removeRoute("a");
        assertResponse("/a", ResponseCode.NOT_FOUND, "");

        context.addRoutes(routeA());
        assertResponse("/a", ResponseCode.CONTENT, "reply-a");
    }

    private void assertResponse(String path, ResponseCode code, String text) throws Exception {
        CoapClient client = createClient(path);
        try {
            CoapResponse response = client.post("x", MediaTypeRegistry.TEXT_PLAIN);
            assertNotNull(response, "no response for " + path);
            assertEquals(code, response.getCode(), "response code for " + path);
            assertEquals(text, response.getResponseText(), "response for " + path);
        } finally {
            client.shutdown();
        }
    }

    private RouteBuilder routeA() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                fromF("coap://localhost:%d/a", PORT.getPort()).routeId("a").setBody(constant("reply-a"));
            }
        };
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                fromF("coap://localhost:%d/a", PORT.getPort()).routeId("a").setBody(constant("reply-a"));
                fromF("coap://localhost:%d/b/c", PORT.getPort()).routeId("bc").setBody(constant("reply-bc"));
                fromF("coap://localhost:%d/d?coapMethodRestrict=GET", PORT.getPort()).routeId("dGet")
                        .setBody(constant("reply-dGet"));
                fromF("coap://localhost:%d/d?coapMethodRestrict=POST", PORT.getPort()).routeId("dPost")
                        .setBody(constant("reply-dPost"));

                restConfiguration().component("coap").port(PORT.getPort());
                rest("/orders").post("/{id}").to("direct:order");
                from("direct:order").setBody(simple("order ${header.id}"));
            }
        };
    }
}
