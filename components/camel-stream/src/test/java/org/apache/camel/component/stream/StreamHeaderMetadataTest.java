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
package org.apache.camel.component.stream;

import org.apache.camel.CatalogCamelContext;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The stream header of kind header is in the metadata of the component.
 */
public class StreamHeaderMetadataTest extends CamelTestSupport {

    @Test
    public void testStreamHeaderIsDocumented() throws Exception {
        JsonObject schema = (JsonObject) Jsoner.deserialize(
                ((CatalogCamelContext) context).getComponentParameterJsonSchema("stream"));
        JsonObject headers = schema.getMap("headers");
        JsonObject stream = headers.getMap(StreamConstants.STREAM);
        assertNotNull(stream, "the stream header");
        assertEquals("producer", stream.getString("label"));
        assertEquals("java.io.OutputStream", stream.getString("javaType"));
    }
}
