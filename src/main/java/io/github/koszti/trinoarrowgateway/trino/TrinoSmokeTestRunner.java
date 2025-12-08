package io.github.koszti.trinoarrowgateway.trino;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class TrinoSmokeTestRunner
        implements CommandLineRunner
{
    private static final Logger log = LoggerFactory.getLogger(TrinoSmokeTestRunner.class);

    private final TrinoClient trinoClient;

    public TrinoSmokeTestRunner(TrinoClient trinoClient) {
        this.trinoClient = trinoClient;
    }

    @Override
    public void run(String... args) {
        try {
            TrinoQueryHandle handle = trinoClient.submitQuery("SELECT 1");
            log.info("Trino smoke test OK. queryId={}", handle.getQueryId());
        } catch (Exception e) {
            log.warn("Trino smoke test failed (this is OK in dev if Trino is not running): {}", e.toString());
        }
    }
}
