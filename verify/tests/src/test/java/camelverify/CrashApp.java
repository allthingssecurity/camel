package camelverify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.processor.idempotent.FileIdempotentRepository;

/**
 * Child JVM for R1: the route of camel-quarkus-examples/jpa-idempotent-repository, with the JPA repository replaced by
 * FileIdempotentRepository (also persists the key in add()) and the HTTP call replaced by an append to a log file.
 * When the exchange carrying crashBody reaches crashPoint, the JVM is halted (no onCompletion, no shutdown hooks).
 * args: inbox repoFile apiLog eager(true|false) crashPoint(beforeApi|afterApi|none) crashBody
 */
public final class CrashApp {

    private CrashApp() {
    }

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(route(args[0], Path.of(args[1]), Path.of(args[2]), Boolean.parseBoolean(args[3]), args[4],
                args[5]));
        context.start();
        // wait for the crash point to halt this JVM; if it never comes, exit with 2
        new CountDownLatch(1).await(30, TimeUnit.SECONDS);
        System.exit(2); // no crash happened
    }

    static RouteBuilder route(
            String inbox, Path repoFile, Path apiLog, boolean eager, String crashPoint, String crashBody) {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + inbox + "?initialDelay=0&delay=50&sortBy=file:name")
                        .idempotentConsumer(simple("${body}"))
                        .idempotentRepository(FileIdempotentRepository.fileIdempotentRepository(repoFile.toFile()))
                        .eager(eager)
                        .process(e -> crashIf(e, "beforeApi", crashPoint, crashBody))
                        .process(e -> callCostlyApi(e, apiLog))
                        .process(e -> crashIf(e, "afterApi", crashPoint, crashBody));
            }
        };
    }

    static void crashIf(Exchange e, String here, String crashPoint, String crashBody) {
        if (here.equals(crashPoint) && crashBody.equals(e.getIn().getBody(String.class))) {
            Runtime.getRuntime().halt(137);
        }
    }

    static void callCostlyApi(Exchange e, Path apiLog) throws IOException {
        Files.writeString(apiLog, e.getIn().getBody(String.class) + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
    }
}
