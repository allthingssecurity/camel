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
package org.apache.camel.component.ibm.cos;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.auth.BasicAWSCredentials;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The ibm-cos consumer and the getObject operation set CamelIBMCOSContentLength to the size of the object they read.
 * When a route changes the body and stores it with an ibm-cos producer, that header is stale: the COS SDK checks that
 * the body has the declared content length (LengthCheckInputStream), so a single part upload of a longer or shorter
 * body failed. A multipart upload splits the declared length into parts (InputSubstream), so a longer body was stored
 * cut to the header length. The real COS client runs against an in-process fake S3 server that stores what it receives.
 */
class IBMCOSProducerStaleContentLengthTest extends CamelTestSupport {

    @TempDir
    Path dir;

    private FakeS3Server s3;
    private AmazonS3 cosClient;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        s3 = new FakeS3Server("bkt");
        s3.start();
        cosClient = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("xxx", "yyy")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3.endpoint(), "us-south"))
                .withPathStyleAccessEnabled(true)
                .build();
        CamelContext context = super.createCamelContext();
        context.getRegistry().bind("cos", cosClient);
        return context;
    }

    @AfterEach
    void stopServer() {
        if (cosClient != null) {
            cosClient.shutdown();
        }
        if (s3 != null) {
            s3.stop();
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:put").to("ibm-cos://bkt?cosClient=#cos&autoCreateBucket=false");
                from("direct:multipart")
                        .to("ibm-cos://bkt?cosClient=#cos&autoCreateBucket=false&multiPartUpload=true&partSize=16");
            }
        };
    }

    @Test
    void bodyLongerThanAStaleContentLengthHeaderIsStoredWhole() {
        String text = "a body made longer by the route";

        Exchange result = put("changed.txt", 10L, text);

        assertNull(result.getException());
        assertEquals(text, stored("changed.txt"), "the whole body must be stored, not the length of the header");
    }

    @Test
    void bodyShorterThanAStaleContentLengthHeaderIsStoredWithItsLength() {
        Exchange result = put("short.txt", 1000L, "short".getBytes(StandardCharsets.UTF_8));

        assertNull(result.getException(), "a body shorter than the header must be stored");
        assertEquals("short", stored("short.txt"));
    }

    @Test
    void fileBodyIsStoredWithItsLength() throws Exception {
        File file = Files.writeString(dir.resolve("data.txt"), "the content of the file").toFile();

        Exchange result = put("file.txt", 3L, file);

        assertNull(result.getException());
        assertEquals("the content of the file", stored("file.txt"));
    }

    @Test
    void multipartUploadOfABodyLongerThanAStaleContentLengthHeaderIsStoredWhole() {
        // the header (20) is above the part size (16), so the upload is a multipart upload, whose parts only cover the
        // declared length
        String text = "a body made longer by the route";

        Exchange result = template.send("direct:multipart", exchange -> {
            exchange.getIn().setHeader(IBMCOSConstants.KEY, "multipart.txt");
            exchange.getIn().setHeader(IBMCOSConstants.CONTENT_LENGTH, 20L);
            exchange.getIn().setBody(text);
        });

        assertNull(result.getException());
        assertEquals(text, stored("multipart.txt"), "the whole body must be stored, not the length of the header");
    }

    @Test
    void streamOfUnknownLengthUsesTheContentLengthHeader() {
        // control: the case the header is for, a stream whose length is not known (such as the content of an
        // object got by the getObject operation)
        byte[] data = "a stream of unknown length".getBytes(StandardCharsets.UTF_8);
        InputStream stream = new FilterInputStream(new ByteArrayInputStream(data)) {
        };

        Exchange result = put("stream.txt", (long) data.length, stream);

        assertNull(result.getException());
        assertEquals("a stream of unknown length", stored("stream.txt"));
    }

    private Exchange put(String key, long contentLengthHeader, Object body) {
        return template.send("direct:put", exchange -> {
            exchange.getIn().setHeader(IBMCOSConstants.KEY, key);
            exchange.getIn().setHeader(IBMCOSConstants.CONTENT_LENGTH, contentLengthHeader);
            exchange.getIn().setBody(body);
        });
    }

    private String stored(String key) {
        byte[] content = s3.content(key);
        return content != null ? new String(content, StandardCharsets.UTF_8) : null;
    }
}
