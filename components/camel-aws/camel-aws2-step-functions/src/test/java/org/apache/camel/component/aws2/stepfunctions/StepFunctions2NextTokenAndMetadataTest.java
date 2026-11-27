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
package org.apache.camel.component.aws2.stepfunctions;

import java.util.List;

import org.apache.camel.BindToRegistry;
import org.apache.camel.CatalogCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryRequest;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryResponse;
import software.amazon.awssdk.services.sfn.model.ListActivitiesRequest;
import software.amazon.awssdk.services.sfn.model.ListActivitiesResponse;
import software.amazon.awssdk.services.sfn.model.ListExecutionsRequest;
import software.amazon.awssdk.services.sfn.model.ListExecutionsResponse;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesRequest;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The CamelAwsStepFunctionsNextToken header selects the page of the list operations and of getExecutionHistory, and the
 * catalog metadata of the operation option and of the credentials options.
 */
public class StepFunctions2NextTokenAndMetadataTest extends CamelTestSupport {

    private static final List<String> OPERATIONS
            = List.of("listStateMachines", "listActivities", "listExecutions", "getExecutionHistory");

    @BindToRegistry("awsSfnClient")
    private final RecordingSfnClient client = new RecordingSfnClient();

    @Test
    public void testNextTokenHeaderIsSent() {
        for (String operation : OPERATIONS) {
            template.sendBodyAndHeader("direct:" + operation, null, StepFunctions2Constants.NEXT_TOKEN, "page-2");
        }

        assertEquals("page-2", client.listStateMachines.nextToken());
        assertEquals("page-2", client.listActivities.nextToken());
        assertEquals("page-2", client.listExecutions.nextToken());
        assertEquals("page-2", client.getExecutionHistory.nextToken());
    }

    @Test
    public void testNoNextTokenWithoutHeader() {
        for (String operation : OPERATIONS) {
            template.sendBody("direct:" + operation, null);
        }

        assertNull(client.listStateMachines.nextToken());
        assertNull(client.listActivities.nextToken());
        assertNull(client.listExecutions.nextToken());
        assertNull(client.getExecutionHistory.nextToken());
    }

    @Test
    public void testOperationNotRequiredAndCredentialsLabels() throws Exception {
        JsonObject schema = (JsonObject) Jsoner.deserialize(
                ((CatalogCamelContext) context).getComponentParameterJsonSchema("aws2-step-functions"));
        JsonObject properties = schema.getMap("properties");

        JsonObject operation = properties.getMap("operation");
        assertFalse(operation.getBoolean("required"));
        for (String name : List.of("useDefaultCredentialsProvider", "useProfileCredentialsProvider",
                "profileCredentialsName")) {
            JsonObject option = properties.getMap(name);
            assertEquals("security", option.getString("label"), name);
        }
        JsonObject headers = schema.getMap("headers");
        JsonObject nextToken = headers.getMap(StepFunctions2Constants.NEXT_TOKEN);
        assertEquals("listStateMachines listActivities listExecutions getExecutionHistory", nextToken.getString("label"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                for (String operation : OPERATIONS) {
                    from("direct:" + operation)
                            .to("aws2-step-functions://test?awsSfnClient=#awsSfnClient&operation=" + operation);
                }
            }
        };
    }

    private static final class RecordingSfnClient implements SfnClient {
        private volatile ListStateMachinesRequest listStateMachines;
        private volatile ListActivitiesRequest listActivities;
        private volatile ListExecutionsRequest listExecutions;
        private volatile GetExecutionHistoryRequest getExecutionHistory;

        @Override
        public ListStateMachinesResponse listStateMachines(ListStateMachinesRequest request) {
            listStateMachines = request;
            return ListStateMachinesResponse.builder().build();
        }

        @Override
        public ListActivitiesResponse listActivities(ListActivitiesRequest request) {
            listActivities = request;
            return ListActivitiesResponse.builder().build();
        }

        @Override
        public ListExecutionsResponse listExecutions(ListExecutionsRequest request) {
            listExecutions = request;
            return ListExecutionsResponse.builder().build();
        }

        @Override
        public GetExecutionHistoryResponse getExecutionHistory(GetExecutionHistoryRequest request) {
            getExecutionHistory = request;
            return GetExecutionHistoryResponse.builder().build();
        }

        @Override
        public String serviceName() {
            return "sfn";
        }

        @Override
        public void close() {
            // nothing to close
        }
    }
}
