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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.qdrant.client.PointIdFactory;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.Points;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.qdrant.QdrantAction;
import org.apache.camel.component.qdrant.QdrantHeaders;

public class RAGUpsert implements Processor {

    private String payloadKey = "content";
    private String textVariable = "text";

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        List<Float> embedding = exchange.getIn().getBody(List.class);
        String text = exchange.getVariable(textVariable, String.class);

        Points.PointStruct point = Points.PointStruct.newBuilder()
                // a random id, as in the embeddings data type transformer. The upsert replaces a point with the
                // same id, so ids counted from 1 by each instance replaced the points of an earlier run (a restart)
                // or of another instance writing to the same collection
                .setId(PointIdFactory.id(UUID.randomUUID()))
                .setVectors(VectorsFactory.vectors(embedding))
                .putAllPayload(Map.of(payloadKey, ValueFactory.value(text)))
                .build();

        exchange.getIn().setBody(List.of(point));
        exchange.getIn().setHeader(QdrantHeaders.ACTION, QdrantAction.UPSERT);
    }

    public String getPayloadKey() {
        return payloadKey;
    }

    public void setPayloadKey(String payloadKey) {
        this.payloadKey = payloadKey;
    }

    public String getTextVariable() {
        return textVariable;
    }

    public void setTextVariable(String textVariable) {
        this.textVariable = textVariable;
    }
}
