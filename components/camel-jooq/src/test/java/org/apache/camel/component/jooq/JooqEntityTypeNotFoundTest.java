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
package org.apache.camel.component.jooq;

import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A class name in the URI path that cannot be found fails the consumer and the producer with the operation NONE when
 * they are created, with the class name in the message. The producer with a plain SQL query does not use the entityType
 * and still works.
 */
public class JooqEntityTypeNotFoundTest extends BaseJooqTest {

    private static final String MISSING = "org.apache.camel.component.jooq.db.tables.records.BokStoreRecord";

    @Test
    public void testConsumerFailsWithClassName() {
        Endpoint endpoint = context.getEndpoint("jooq://" + MISSING);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> endpoint.createConsumer(exchange -> {
                }));
        assertTrue(e.getMessage().contains(MISSING), e.getMessage());
    }

    @Test
    public void testProducerNoneFailsWithClassName() {
        Endpoint endpoint = context.getEndpoint("jooq://" + MISSING + "?operation=none");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, endpoint::createProducer);
        assertTrue(e.getMessage().contains(MISSING), e.getMessage());
    }

    @Test
    public void testProducerWithQueryDoesNotNeedEntityType() {
        Result<?> result = (Result<?>) template.sendBody("direct:select", ExchangePattern.InOut, null);
        assertEquals(0, result.size());
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        JooqComponent jooqComponent = (JooqComponent) context().getComponent("jooq");
        JooqConfiguration jooqConfiguration = new JooqConfiguration();
        jooqConfiguration.setDatabaseConfiguration(create.configuration());
        jooqComponent.setConfiguration(jooqConfiguration);

        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:select")
                        .to("jooq://" + MISSING + "?operation=fetch&query=select * from book_store x where x.name = 'none'");
            }
        };
    }
}
