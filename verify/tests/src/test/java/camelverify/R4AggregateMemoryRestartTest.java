package camelverify;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.aggregate.MemoryAggregationRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R4: camel-examples/aggregate camel-context.xml (stream:in replaced by direct:in, stream:out by a mock). The README
 * says the example resumes after ctrl+c because it uses a persistent store; the configured repository is
 * MemoryAggregationRepository.
 */
class R4AggregateMemoryRestartTest {

    /** copy of org.apache.camel.example.NumberAggregationStrategy */
    static class NumberAggregationStrategy implements AggregationStrategy {
        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            if (oldExchange == null) {
                return newExchange;
            }
            String input = newExchange.getIn().getBody(String.class);
            if ("STOP".equalsIgnoreCase(input)) {
                return oldExchange;
            }
            Integer num1 = oldExchange.getIn().getBody(Integer.class);
            Integer num2 = newExchange.getIn().getBody(Integer.class);
            Integer num3 = (num1 != null ? num1 : 0) + (num2 != null ? num2 : 0);
            oldExchange.getIn().setBody(num3);
            return oldExchange;
        }
    }

    static CamelContext start(MemoryAggregationRepository repo) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:in")
                        .aggregate(constant(true), new NumberAggregationStrategy())
                        .aggregationRepository(repo).eagerCheckCompletion()
                        .completionPredicate(simple("${body} contains 'STOP'"))
                        .transform(simple("The result is: ${body}"))
                        .to("mock:out");
            }
        });
        context.start();
        return context;
    }

    @Test
    void enteredNumbersAreForgottenAcrossARestart() throws Exception {
        CamelContext first = start(new MemoryAggregationRepository());
        first.createProducerTemplate().sendBody("direct:in", "1");
        first.stop(); // ctrl + c

        CamelContext second = start(new MemoryAggregationRepository()); // the example started again
        ProducerTemplate t = second.createProducerTemplate();
        MockEndpoint out = second.getEndpoint("mock:out", MockEndpoint.class);
        out.expectedMessageCount(1);
        t.sendBody("direct:in", "2");
        t.sendBody("direct:in", "STOP");
        out.assertIsSatisfied();
        assertThat(out.getReceivedExchanges()).hasSize(1);
        // the README's promise would print 3
        assertThat(out.getReceivedExchanges().get(0).getIn().getBody(String.class)).isEqualTo("The result is: 2");
        second.stop();
    }

    @Test
    void sameRepositoryInstanceIsClearedOnStopToo() throws Exception {
        MemoryAggregationRepository repo = new MemoryAggregationRepository();
        CamelContext context = start(repo);
        context.createProducerTemplate().sendBody("direct:in", "1");
        context.stop();
        assertThat(repo.getKeys()).isEmpty();
    }
}
