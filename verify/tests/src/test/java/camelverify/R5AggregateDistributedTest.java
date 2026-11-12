package camelverify;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.aggregate.jdbc.JdbcAggregationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * R5: camel-examples/aggregate-dist. Two Camel instances share one H2 database (JdbcAggregationRepository,
 * useRecovery=false) and aggregate items 1..END of one correlation id concurrently.
 */
class R5AggregateDistributedTest {

    static final int END = 60;

    record Result(List<String> completed, int failedSends) {
    }

    Result run(String db, boolean optimistic) throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource("jdbc:h2:mem:" + db + ";DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table aggregation(id varchar(255) not null primary key, exchange blob not null,"
                     + " version bigint not null)");
        jdbc.execute("create table aggregation_completed(id varchar(255) not null primary key,"
                     + " exchange blob not null, version bigint not null)");
        List<CamelContext> nodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            nodes.add(node(ds, optimistic));
        }
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger next = new AtomicInteger(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        for (int t = 0; t < 4; t++) {
            ProducerTemplate template = nodes.get(t % 2).createProducerTemplate();
            pool.execute(() -> {
                int item;
                while ((item = next.getAndIncrement()) <= END) {
                    try {
                        template.sendBodyAndHeader("direct:aggregator", item, "corrId", "c1");
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        if (optimistic) {
            // the completed group is sent on the aggregator's own thread: wait for it before stopping the nodes
            await().atMost(10, TimeUnit.SECONDS).until(() -> received(nodes) >= 1);
        }
        List<String> completed = new ArrayList<>();
        for (CamelContext n : nodes) {
            for (Exchange e : n.getEndpoint("mock:result", MockEndpoint.class).getReceivedExchanges()) {
                completed.add(e.getIn().getBody(String.class));
            }
            n.stop();
        }
        return new Result(completed, failed.get());
    }

    static int received(List<CamelContext> nodes) {
        int n = 0;
        for (CamelContext c : nodes) {
            n += c.getEndpoint("mock:result", MockEndpoint.class).getReceivedCounter();
        }
        return n;
    }

    static CamelContext node(DataSource ds, boolean optimistic) throws Exception {
        JdbcAggregationRepository repo
                = new JdbcAggregationRepository(new DataSourceTransactionManager(ds), "aggregation", ds);
        repo.setUseRecovery(false);
        repo.setStoreBodyAsText(false);
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                var agg = from("direct:aggregator")
                        .filter(body().isNotNull())
                        .aggregate().header("corrId")
                        .aggregationStrategy((o, n) -> {
                            if (o == null) {
                                return n;
                            }
                            o.getIn().setBody(o.getIn().getBody(String.class) + "." + n.getIn().getBody(String.class));
                            return o;
                        })
                        .completionPredicate(e -> e.getIn().getBody(String.class).split("\\.").length == END)
                        .aggregationRepository(repo);
                if (optimistic) {
                    agg.optimisticLocking();
                }
                agg.to("mock:result");
            }
        });
        context.start();
        return context;
    }

    static Set<Integer> items(String body) {
        Set<Integer> s = new TreeSet<>();
        for (String p : body.split("\\.")) {
            s.add(Integer.parseInt(p));
        }
        return s;
    }

    @Test
    void optimisticLockingCompletesTheGroupWithEveryItem() throws Exception {
        Result r = run("opt", true);
        System.out.println("R5 optimistic: completed=" + r.completed().size() + " failedSends=" + r.failedSends());
        assertThat(r.failedSends()).isZero();
        assertThat(r.completed()).hasSize(1);
        assertThat(items(r.completed().get(0))).hasSize(END);
    }

    @Test
    void withoutOptimisticLockingConcurrentInstancesFailSends() throws Exception {
        Result r = run("noopt", false);
        System.out.println("R5 no optimistic locking: completed=" + r.completed().size() + " failedSends="
                           + r.failedSends());
        assertThat(r.failedSends()).isPositive();
        assertThat(r.completed()).isEmpty();
    }
}
