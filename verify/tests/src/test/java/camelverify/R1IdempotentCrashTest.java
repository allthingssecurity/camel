package camelverify;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * R1: file consumer + idempotentConsumer (persistent repository) + costly call.
 * Phase 1 runs in a child JVM that is halted at the crash point; phase 2 restarts the same route in this JVM.
 */
class R1IdempotentCrashTest {

    @TempDir
    Path dir;

    @Test
    void eagerPersistentRepositoryLosesTheMessageThatWasInFlightAtTheCrash() throws Exception {
        List<String> calls = runCrashThenRestart(true, "beforeApi");
        // B was never sent to the API: after the restart its key was already in the repository, the
        // file was treated as a duplicate and moved to .camel
        assertThat(calls).containsExactly("A");
        assertThat(dir.resolve("inbox/.camel/f3.txt")).exists();
    }

    @Test
    void nonEagerPersistentRepositoryCallsTheApiTwiceForTheMessageInFlightAtTheCrash() throws Exception {
        List<String> calls = runCrashThenRestart(false, "afterApi");
        assertThat(calls).containsExactly("A", "B", "B");
    }

    @Test
    void eagerPersistentRepositoryWithoutCrashCallsOncePerBody() throws Exception {
        Path inbox = writeInbox();
        restartAndDrain(inbox, true);
        assertThat(Files.readAllLines(dir.resolve("api.log"))).containsExactly("A", "B");
    }

    private Path writeInbox() throws Exception {
        Path inbox = Files.createDirectories(dir.resolve("inbox"));
        // f1 and f2 have the same body (a duplicate), f3 a different one
        Files.writeString(dir.resolve("f1.tmp"), "A");
        Files.writeString(dir.resolve("f2.tmp"), "A");
        Files.writeString(dir.resolve("f3.tmp"), "B");
        for (String f : List.of("f1", "f2", "f3")) {
            Files.move(dir.resolve(f + ".tmp"), inbox.resolve(f + ".txt"));
        }
        return inbox;
    }

    private List<String> runCrashThenRestart(boolean eager, String crashPoint) throws Exception {
        Path inbox = writeInbox();
        Path repo = dir.resolve("repo.dat");
        Path api = dir.resolve("api.log");
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        Process p = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), CrashApp.class.getName(),
                inbox.toString(), repo.toString(), api.toString(), String.valueOf(eager), crashPoint, "B")
                .redirectErrorStream(true).redirectOutput(dir.resolve("child.log").toFile()).start();
        assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).as("child must have crashed at the crash point").isEqualTo(137);
        assertThat(inbox.resolve("f3.txt")).as("f3 still in the inbox after the crash").exists();
        assertThat(Files.readAllLines(repo)).contains(eager ? "B" : "A");

        restartAndDrain(inbox, eager);
        return Files.readAllLines(api);
    }

    private void restartAndDrain(Path inbox, boolean eager) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(CrashApp.route(inbox.toString(), dir.resolve("repo.dat"), dir.resolve("api.log"), eager,
                "none", "none"));
        context.start();
        try {
            await().atMost(20, TimeUnit.SECONDS).until(() -> !Files.exists(inbox.resolve("f1.txt"))
                    && !Files.exists(inbox.resolve("f2.txt")) && !Files.exists(inbox.resolve("f3.txt")));
            await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size() == 0);
        } finally {
            context.stop();
        }
    }
}
