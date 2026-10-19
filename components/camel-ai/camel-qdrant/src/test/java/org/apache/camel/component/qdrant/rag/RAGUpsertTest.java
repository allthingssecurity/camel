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
package org.apache.camel.component.qdrant.rag;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The upsert replaces a point that has the same id. A RAGUpsert used to number its points from 1, so the points of a
 * new run (a restart, or a second RAGUpsert writing to the same collection) replaced the documents stored before.
 */
class RAGUpsertTest {

    private final DefaultCamelContext context = new DefaultCamelContext();

    @Test
    void anotherRunDoesNotReuseThePointIds() throws Exception {
        Set<Common.PointId> ids = new HashSet<>();
        // the first run stores two documents
        RAGUpsert firstRun = new RAGUpsert();
        ids.add(pointId(firstRun, "first document"));
        ids.add(pointId(firstRun, "second document"));

        // after a restart, a new RAGUpsert stores two more documents into the same collection
        RAGUpsert secondRun = new RAGUpsert();
        ids.add(pointId(secondRun, "third document"));
        ids.add(pointId(secondRun, "fourth document"));

        assertThat(ids).as("every stored document needs its own point id, or the upsert replaces an earlier one")
                .hasSize(4);
    }

    private Common.PointId pointId(RAGUpsert upsert, String text) throws Exception {
        Exchange exchange = exchange(text);
        upsert.process(exchange);
        return point(exchange).getId();
    }

    private Exchange exchange(String text) {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody(List.of(0.1f, 0.2f, 0.3f));
        exchange.setVariable("text", text);
        return exchange;
    }

    @SuppressWarnings("unchecked")
    private static Points.PointStruct point(Exchange exchange) {
        return ((List<Points.PointStruct>) exchange.getIn().getBody(List.class)).get(0);
    }
}
