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
package org.apache.camel.component.jcache;

import java.util.Map;

import javax.cache.Cache;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing a route that uses a jcache endpoint must not close the caches that the other routes still use.
 */
class JCacheRemoveRouteTest extends JCacheComponentTestSupport {

    @Test
    void removingARouteKeepsTheCachesOfTheOtherRoutes() throws Exception {
        put("direct:a", "k", "v");
        put("direct:temp", "t", "x");

        context.getRouteController().stopRoute("temp");
        context.removeRoute("temp");

        assertEquals("v", get("direct:a", "k"));
    }

    @Test
    void removingTheConsumerRouteOfACacheKeepsItsProducer() throws Exception {
        MockEndpoint events = getMockEndpoint("mock:events");
        events.expectedMessageCount(1);
        put("direct:b", "k1", "v1");
        events.assertIsSatisfied();

        context.getRouteController().stopRoute("events");
        context.removeRoute("events");

        put("direct:b", "k2", "v2");
        assertEquals("v2", get("direct:b", "k2"));
    }

    @Test
    void theLastManagerOfACacheClosesIt() throws Exception {
        JCacheConfiguration conf = new JCacheConfiguration();
        conf.setCacheName(randomString());
        JCacheManager<Object, Object> first = new JCacheManager<>(conf);
        JCacheManager<Object, Object> second = new JCacheManager<>(conf);
        Cache<Object, Object> cache = first.getCache();
        assertEquals(cache, second.getCache());

        first.close();
        assertFalse(cache.isClosed());
        cache.put("k", "v");

        second.close();
        assertTrue(cache.isClosed());
    }

    private void put(String uri, String key, String value) {
        template.sendBodyAndHeaders(uri, value,
                Map.of(JCacheConstants.ACTION, "PUT", JCacheConstants.KEY, key));
    }

    private Object get(String uri, String key) {
        return template.requestBodyAndHeaders(uri, null,
                Map.of(JCacheConstants.ACTION, "GET", JCacheConstants.KEY, key));
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:a").routeId("a").to("jcache://remove-route-a");
                from("direct:temp").routeId("temp").to("jcache://remove-route-temp");

                from("direct:b").routeId("b").to("jcache://remove-route-b");
                from("jcache://remove-route-b?filteredEvents=UPDATED,REMOVED,EXPIRED").routeId("events")
                        .to("mock:events");
            }
        };
    }
}
