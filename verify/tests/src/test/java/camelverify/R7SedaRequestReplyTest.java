package camelverify;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** R7: seda request-reply from the seda component docs (mina replaced by direct), and a variant with a cycle. */
class R7SedaRequestReplyTest {

    @Test
    void docExampleRepliesToEveryRequest() throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:tcp").to("seda:input");
                from("seda:input").transform(simple("reply to ${body}"));
            }
        });
        context.start();
        assertThat(context.createProducerTemplate().requestBody("direct:tcp", "hi", String.class))
                .isEqualTo("reply to hi");
        context.stop();
    }

    @Test
    void routeThatCallsItsOwnSedaQueueInOutWaitsUntilTimeout() throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:tcp").to("seda:input?timeout=3000");
                from("seda:input?concurrentConsumers=1")
                        .choice().when(header("nested").isNull())
                            .setHeader("nested", constant(true))
                            .to(ExchangePattern.InOut, "seda:input?timeout=1500")
                        .end()
                        .transform(simple("reply to ${body}"));
            }
        });
        context.start();
        long start = System.nanoTime();
        assertThatThrownBy(() -> context.createProducerTemplate().requestBody("direct:tcp", "hi", String.class))
                .isInstanceOf(CamelExecutionException.class)
                .hasRootCauseInstanceOf(ExchangeTimedOutException.class);
        System.out.println("R7 cycle: failed after ms=" + (System.nanoTime() - start) / 1_000_000);
        context.stop();
    }
}
