package camelverify;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R3 (partial): camel-quarkus-examples/kafka hands records to seda:kafka-messages, drained only by a REST call with
 * ConsumerTemplate. Kafka itself is replaced by direct (no broker offline): this only confirms the seda half, that
 * messages buffered there do not survive a stop of the application. The Kafka offset side is from the model + source.
 */
class R3SedaBufferStopTest {

    @Test
    void bufferedMessagesAreGoneAfterAStop() throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:kafkaStandIn").routeId("FromKafka2Seda").to("seda:kafka-messages");
            }
        });
        context.start();
        for (int i = 1; i <= 3; i++) {
            context.createProducerTemplate().sendBody("direct:kafkaStandIn", "Message #" + i);
        }
        // the REST endpoint was called once before the stop
        assertThat(context.createConsumerTemplate().receiveBody("seda:kafka-messages", 1000, String.class))
                .isEqualTo("Message #1");
        long t0 = System.nanoTime();
        context.stop();
        System.out.println("R3 stop took ms=" + (System.nanoTime() - t0) / 1_000_000);
        context.start();
        assertThat(context.createConsumerTemplate().receiveBody("seda:kafka-messages", 1000, String.class)).isNull();
        context.stop();
    }
}
