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
package org.apache.camel.component.jsonata;

import com.dashjoin.jsonata.Jsonata;
import org.apache.camel.Exchange;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * When the producer reads the resource of the endpoint, and when it parses the expression again.
 */
class JsonataExpressionResourceTest extends CamelTestSupport {

    private static final String SPEC = "classpath:org/apache/camel/component/jsonata/firstSample/expressions.spec";

    private Exchange send(String uri, String body) {
        return template.request(uri, exchange -> exchange.getIn().setBody(body));
    }

    @Test
    void headerExpressionDoesNotNeedTheEndpointResource() {
        // the endpoint resource was opened before the header was read, so a missing resource failed the exchange,
        // and with contentCache=false the stream of an existing one was never closed
        Exchange result = template.request(
                "jsonata:classpath:org/apache/camel/component/jsonata/missing.spec"
                                           + "?allowTemplateFromHeader=true&contentCache=false&outputType=JsonString",
                exchange -> {
                    exchange.getIn().setHeader(JsonataConstants.JSONATA_RESOURCE_URI, "name");
                    exchange.getIn().setBody("{\"name\":\"Camel\"}");
                });

        assertNull(result.getException(), () -> "the exchange failed with " + result.getException());
        assertEquals("\"Camel\"", result.getMessage().getBody(String.class));
    }

    @Test
    void expressionIsParsedOnceWithContentCache() {
        JsonataEndpoint endpoint = context.getEndpoint("jsonata:" + SPEC + "?contentCache=true", JsonataEndpoint.class);

        send("jsonata:" + SPEC + "?contentCache=true", "{\"FirstName\":\"Fred\"}");
        Jsonata first = endpoint.getExpression();
        send("jsonata:" + SPEC + "?contentCache=true", "{\"FirstName\":\"Wilma\"}");

        // the expression was parsed again for each exchange
        assertNotNull(first);
        assertSame(first, endpoint.getExpression());
    }

    @Test
    void clearingTheContentCacheDropsTheExpression() {
        JsonataEndpoint endpoint = context.getEndpoint("jsonata:" + SPEC + "?contentCache=true", JsonataEndpoint.class);
        send("jsonata:" + SPEC + "?contentCache=true", "{\"FirstName\":\"Fred\"}");

        endpoint.clearContentCache();

        assertNull(endpoint.getExpression());
    }

    @Test
    void expressionIsNotKeptWithoutContentCache() {
        JsonataEndpoint endpoint = context.getEndpoint("jsonata:" + SPEC + "?contentCache=false", JsonataEndpoint.class);

        send("jsonata:" + SPEC + "?contentCache=false", "{\"FirstName\":\"Fred\"}");

        assertNull(endpoint.getExpression());
    }

    @Test
    void allowContextMapAllIsStillAccepted() {
        // the option is no longer in the metadata, as jsonata never used it, but a uri that sets it still works
        JsonataEndpoint endpoint = context.getEndpoint("jsonata:" + SPEC + "?allowContextMapAll=true", JsonataEndpoint.class);

        assertNotNull(endpoint);
    }
}
