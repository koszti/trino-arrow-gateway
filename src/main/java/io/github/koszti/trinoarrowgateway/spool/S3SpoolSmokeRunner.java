package io.github.koszti.trinoarrowgateway.spool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

//@Profile("spool-smoke")
@Component
public class S3SpoolSmokeRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(S3SpoolSmokeRunner.class);

    private final SpoolSource spoolSource;

    public S3SpoolSmokeRunner(SpoolSource spoolSource) {
        this.spoolSource = spoolSource;
    }

    @Override
    public void run(String... args) {
        try {
            // For now just list under some dev prefix, e.g. "demo"
            List<SpoolObject> objects = spoolSource.listObjects("demo");
            log.info("S3 spool smoke test: found {} objects under relative prefix 'demo'", objects.size());
            objects.stream().limit(10).forEach(o -> log.info("  {}", o));
        } catch (Exception e) {
            log.warn("S3 spool smoke test failed (OK if S3 not set up in dev): {}", e.toString());
        }
    }
}
