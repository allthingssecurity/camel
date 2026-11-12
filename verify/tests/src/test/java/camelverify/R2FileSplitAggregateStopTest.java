package camelverify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * R2: camel-quarkus-examples/file-bindy-ftp. bindy CSV unmarshal replaced by a line split, sftp by a mock.
 * A graceful stop between "file moved to .camel" and the 5 s completionInterval loses the books.
 */
class R2FileSplitAggregateStopTest {

    @TempDir
    Path dir;

    static RouteBuilder routes(Path csv, boolean completeAllOnStop) {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + csv + "?delay=1000&initialDelay=0").routeId("files")
                        .split(body().tokenize("\n"))
                        .to("direct:aggregateBooks");
                var route = from("direct:aggregateBooks").routeId("aggregate")
                        .process(e -> e.getIn().setHeader("BookGenre", e.getIn().getBody(String.class).split(",")[1].trim()))
                        .aggregate(header("BookGenre"), new GroupedBodyAggregationStrategy()).completionInterval(5000);
                if (completeAllOnStop) {
                    route.completeAllOnStop();
                }
                route.to("seda:processed");
                from("seda:processed").routeId("upload")
                        .to("mock:sftp");
            }
        };
    }

    private int runOnce(Path csv, boolean completeAllOnStop, boolean dropFile) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(routes(csv, completeAllOnStop));
        context.start();
        MockEndpoint sftp = context.getEndpoint("mock:sftp", MockEndpoint.class);
        if (dropFile) {
            Files.writeString(dir.resolve("books.tmp"), "Dune,SciFi\nSolaris,SciFi");
            Files.move(dir.resolve("books.tmp"), csv.resolve("books.csv"));
            // the file exchange completes as soon as both split parts are stored in the aggregator
            await().atMost(10, TimeUnit.SECONDS).until(() -> Files.exists(csv.resolve(".camel/books.csv")));
        } else {
            await().pollDelay(1500, TimeUnit.MILLISECONDS).atMost(3, TimeUnit.SECONDS).until(() -> true);
        }
        context.stop(); // graceful stop, e.g. a rolling restart, before the completionInterval fires
        return sftp.getReceivedCounter();
    }

    @Test
    void gracefulStopLosesPendingGroupWhoseFileWasAlreadyMoved() throws Exception {
        Path csv = Files.createDirectories(dir.resolve("csv"));
        int before = runOnce(csv, false, true);
        int after = runOnce(csv, false, false); // restart: the file is in .camel, nothing to re-read
        assertThat(before + after).isZero();
        assertThat(csv.resolve("books.csv")).doesNotExist();
    }

    @Test
    void completeAllOnStopUploadsTheGroupOnGracefulStop() throws Exception {
        Path csv = Files.createDirectories(dir.resolve("csv"));
        int before = runOnce(csv, true, true);
        System.out.println("R2 completeAllOnStop uploads on stop: " + before);
        assertThat(before).isEqualTo(1);
    }
}
