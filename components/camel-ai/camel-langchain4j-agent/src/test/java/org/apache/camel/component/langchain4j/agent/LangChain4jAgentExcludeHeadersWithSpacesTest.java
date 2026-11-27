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
package org.apache.camel.component.langchain4j.agent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.mcp.client.McpClient;
import dev.langchain4j.service.Result;
import dev.langchain4j.service.tool.AiServiceTool;
import dev.langchain4j.service.tool.ToolProvider;
import dev.langchain4j.service.tool.ToolProviderRequest;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.langchain4j.agent.api.AgentConfiguration;
import org.apache.camel.component.langchain4j.agent.api.AgentWithoutMemory;
import org.apache.camel.component.langchain4j.agent.api.AiAgentBody;
import org.apache.camel.component.langchain4j.agent.api.Headers;
import org.apache.camel.component.langchain4j.agent.support.StubMcpClient;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tags option and the CamelLangChain4jAgentExcludeTags and CamelLangChain4jAgentExcludeMcpServers headers are lists
 * in which spaces around the commas do not count.
 */
class LangChain4jAgentExcludeHeadersWithSpacesTest extends CamelTestSupport {

    private static final ToolProviderRequest TOOL_REQUEST = new ToolProviderRequest("test", UserMessage.from("hello"));

    private static final ToolSpecification WEATHER_TOOL = ToolSpecification.builder()
            .name("get_weather")
            .description("Returns weather for a city")
            .build();

    private static final ToolSpecification INVENTORY_TOOL = ToolSpecification.builder()
            .name("check_inventory")
            .description("Returns stock levels")
            .build();

    private final AtomicReference<ToolProvider> capturedToolProvider = new AtomicReference<>();

    LangChain4jAgentExcludeHeadersWithSpacesTest() {
        testConfiguration().withUseAdviceWith(true);
    }

    @BeforeEach
    void resetCapturedToolProvider() {
        capturedToolProvider.set(null);
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("ai-tool:toolA?tags=a&description=Tool A").setBody(constant("A"));
                from("ai-tool:toolB?tags=b&description=Tool B").setBody(constant("B"));
                from("direct:tags").to("langchain4j-agent:tags");
                from("direct:mcp").to("langchain4j-agent:mcp");
            }
        };
    }

    @Test
    void excludeTagsHeaderRemovesATagOfTheOptionWrittenWithASpace() {
        configureTagsEndpoint("a, b");

        template.sendBodyAndHeader("direct:tags", new AiAgentBody<>("hello"), Headers.EXCLUDE_TAGS, "b");

        assertThat(toolNames(capturedToolProvider.get())).containsExactly("toolA");
    }

    @Test
    void excludeTagsHeaderWrittenWithASpaceRemovesEveryTag() {
        configureTagsEndpoint("a,b");

        template.sendBodyAndHeader("direct:tags", new AiAgentBody<>("hello"), Headers.EXCLUDE_TAGS, "a, b");

        assertThat(toolNames(capturedToolProvider.get())).isEmpty();
    }

    @Test
    void tagsOptionWrittenWithSpacesOffersTheToolsOfEveryTag() {
        configureTagsEndpoint(" a , b ");

        template.sendBody("direct:tags", new AiAgentBody<>("hello"));

        assertThat(toolNames(capturedToolProvider.get())).containsExactlyInAnyOrder("toolA", "toolB");
    }

    @Test
    void excludeMcpServersHeaderWrittenWithASpaceRemovesEveryServer() {
        configureEndpoint("langchain4j-agent:mcp", null,
                List.of(new StubMcpClient("x", WEATHER_TOOL), new StubMcpClient("y", INVENTORY_TOOL)));
        context.start();

        template.sendBodyAndHeader("direct:mcp", new AiAgentBody<>("hello"), Headers.EXCLUDE_MCP_SERVERS, "x, y");

        assertThat(toolNames(capturedToolProvider.get())).isEmpty();
    }

    private void configureTagsEndpoint(String tags) {
        configureEndpoint("langchain4j-agent:tags", tags, null);
        context.start();
    }

    private void configureEndpoint(String endpointUri, String tags, List<McpClient> mcpClients) {
        LangChain4jAgentEndpoint endpoint = context.getEndpoint(endpointUri, LangChain4jAgentEndpoint.class);
        endpoint.getConfiguration().setTags(tags);
        endpoint.getConfiguration().setMcpClients(mcpClients);
        endpoint.getConfiguration().setAgent(new AgentWithoutMemory(new AgentConfiguration()) {
            @Override
            public Result<String> chat(AiAgentBody<?> aiAgentBody, ToolProvider toolProvider) {
                capturedToolProvider.set(toolProvider);
                return Result.<String> builder().content("ok").build();
            }
        });
    }

    private static Set<String> toolNames(ToolProvider toolProvider) {
        if (toolProvider == null) {
            return Set.of();
        }
        return toolProvider.provideTools(TOOL_REQUEST).aiServiceTools().stream()
                .map(AiServiceTool::name)
                .collect(Collectors.toSet());
    }
}
