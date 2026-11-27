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
package org.apache.camel.component.huaweicloud.image;

import com.huaweicloud.sdk.image.v2.model.ImageTaggingResponseResult;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * An ImageClient set on the endpoint is used without the accessKey, secretKey, projectId and region options, which are
 * only needed when the producer creates the client itself.
 */
public class ImageClientWithoutCredentialsTest extends CamelTestSupport {

    @BindToRegistry("imageClient")
    ImageClientMock imageClient = new ImageClientMock(null);

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:tag")
                        .to("hwcloud-imagerecognition:tagRecognition?imageUrl=https://example.com/image.jpg"
                            + "&imageClient=#imageClient")
                        .to("mock:result");
            }
        };
    }

    @Test
    public void testImageClientWithoutCredentials() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMessageCount(2);

        template.sendBody("direct:tag", "");
        template.sendBody("direct:tag", "");

        mock.assertIsSatisfied();
        for (Exchange exchange : mock.getExchanges()) {
            ImageTaggingResponseResult result
                    = assertInstanceOf(ImageTaggingResponseResult.class, exchange.getMessage().getBody());
            assertEquals(1, result.getTags().size());
            assertEquals(MockResult.TAG_RECOGNITION_RESULT_TAG, result.getTags().get(0).getTag());
        }
    }
}
