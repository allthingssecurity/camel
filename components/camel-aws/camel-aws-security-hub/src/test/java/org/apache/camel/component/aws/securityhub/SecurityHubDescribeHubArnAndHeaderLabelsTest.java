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
package org.apache.camel.component.aws.securityhub;

import org.apache.camel.BindToRegistry;
import org.apache.camel.CatalogCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.securityhub.SecurityHubClient;
import software.amazon.awssdk.services.securityhub.model.DescribeHubRequest;
import software.amazon.awssdk.services.securityhub.model.DescribeHubResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * describeHub takes the ARN of the hub from a header, the operation option is not required, and every header names the
 * operations that use it in its label.
 */
public class SecurityHubDescribeHubArnAndHeaderLabelsTest extends CamelTestSupport {

    private static final String HUB_ARN = "arn:aws:securityhub:eu-west-1:123456789012:hub/default";

    @BindToRegistry("securityHubClient")
    private final SecurityHubClient securityHubClient = mock(SecurityHubClient.class);

    @Test
    public void describeHubSendsTheHubArnOfTheHeader() {
        when(securityHubClient.describeHub(any(DescribeHubRequest.class)))
                .thenReturn(DescribeHubResponse.builder().hubArn(HUB_ARN).build());

        template.sendBodyAndHeader("direct:describeHub", null, SecurityHubConstants.HUB_ARN, HUB_ARN);

        ArgumentCaptor<DescribeHubRequest> captor = ArgumentCaptor.forClass(DescribeHubRequest.class);
        verify(securityHubClient).describeHub(captor.capture());
        assertEquals(HUB_ARN, captor.getValue().hubArn());
    }

    @Test
    public void describeHubWithoutTheHeaderSendsNoHubArn() {
        when(securityHubClient.describeHub(any(DescribeHubRequest.class)))
                .thenReturn(DescribeHubResponse.builder().hubArn(HUB_ARN).build());

        template.sendBody("direct:describeHub", null);

        ArgumentCaptor<DescribeHubRequest> captor = ArgumentCaptor.forClass(DescribeHubRequest.class);
        verify(securityHubClient).describeHub(captor.capture());
        assertNull(captor.getValue().hubArn());
    }

    @Test
    public void operationOptionIsNotRequired() throws Exception {
        JsonObject properties = schema().getMap("properties");
        JsonObject operation = properties.getMap("operation");
        assertFalse(operation.getBoolean("required"), "operation has a default, so it must not be required");
        assertEquals("batchImportFindings", operation.getString("defaultValue"));
    }

    @Test
    public void headersAreLabelledWithTheirOperations() throws Exception {
        JsonObject headers = schema().getMap("headers");
        assertLabel(headers, SecurityHubConstants.OPERATION, "all");
        assertLabel(headers, SecurityHubConstants.FINDINGS, "batchImportFindings");
        assertLabel(headers, SecurityHubConstants.FILTERS, "getFindings");
        assertLabel(headers, SecurityHubConstants.MAX_RESULTS, "getFindings listEnabledProductsForImport getFindingHistory");
        assertLabel(headers, SecurityHubConstants.HUB_ARN, "describeHub");
        for (String name : new String[] {
                SecurityHubConstants.FINDING_IDENTIFIERS, SecurityHubConstants.NOTE, SecurityHubConstants.SEVERITY,
                SecurityHubConstants.WORKFLOW, SecurityHubConstants.VERIFICATION_STATE, SecurityHubConstants.CONFIDENCE,
                SecurityHubConstants.CRITICALITY, SecurityHubConstants.USER_DEFINED_FIELDS,
                SecurityHubConstants.RELATED_FINDINGS, SecurityHubConstants.TYPES }) {
            assertLabel(headers, name, "batchUpdateFindings");
        }
        for (String name : headers.keySet()) {
            JsonObject header = headers.getMap(name);
            assertFalse(header.getString("label").isEmpty(), name + " has no label");
        }
    }

    private static void assertLabel(JsonObject headers, String name, String label) {
        JsonObject header = headers.getMap(name);
        assertEquals(label, header.getString("label"), name);
    }

    private JsonObject schema() throws Exception {
        String json = ((CatalogCamelContext) context).getComponentParameterJsonSchema("aws-security-hub");
        return (JsonObject) Jsoner.deserialize(json);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:describeHub")
                        .to("aws-security-hub://test?securityHubClient=#securityHubClient&operation=describeHub");
            }
        };
    }
}
