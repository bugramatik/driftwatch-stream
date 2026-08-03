package io.driftwatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Driftwatch Stream service. Starts the REST API, the Kafka
 * listener container and the scheduled baseline refresh.
 */
@SpringBootApplication
@EnableScheduling
public class DriftwatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(DriftwatchApplication.class, args);
    }
}
