package io.driftwatch.ingest;

import io.driftwatch.alert.AlertService;
import io.driftwatch.drift.DriftCalculator;
import io.driftwatch.store.BaselineStore;
import io.driftwatch.store.FeatureBaseline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Consumes prediction events, stores them, and every {@code checkEvery} events
 * for a feature recomputes drift against the stored baseline and asks the alert
 * service whether the shift is worth telling anyone about.
 */
@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final BaselineStore store;
    private final AlertService alerts;
    private final DriftCalculator calculator = new DriftCalculator();
    private final int checkEvery;
    private final int windowSize;

    public EventConsumer(BaselineStore store,
                         AlertService alerts,
                         @Value("${driftwatch.check-every:500}") int checkEvery,
                         @Value("${driftwatch.window-size:2000}") int windowSize) {
        this.store = store;
        this.alerts = alerts;
        this.checkEvery = Math.max(1, checkEvery);
        this.windowSize = Math.max(100, windowSize);
    }

    @KafkaListener(topics = "${driftwatch.topic}", groupId = "${driftwatch.group}")
    public void onMessage(String payload) {
        PredictionEvent event;
        try {
            event = PredictionEvent.fromJson(payload);
        } catch (IOException | IllegalArgumentException e) {
            log.warn("dropping malformed prediction event: {}", e.getMessage());
            return;
        }

        store.record(event);

        long seen = store.countSince(event.modelId(), event.featureName(), event.observedAt().minusSeconds(3600));
        if (seen % checkEvery != 0) {
            return;
        }
        evaluate(event.modelId(), event.featureName());
    }

    private void evaluate(String modelId, String featureName) {
        Optional<FeatureBaseline> baseline = store.baseline(modelId, featureName);
        if (baseline.isEmpty()) {
            log.debug("no baseline yet for {}/{}, skipping drift check", modelId, featureName);
            return;
        }
        List<Double> recent = store.recentValues(modelId, featureName, windowSize);
        if (recent.size() < 100) {
            return;
        }
        FeatureBaseline b = baseline.get();
        double psi = calculator.psi(b.bins(), b.proportions(), recent);
        double ks = calculator.ks(b.bins(), b.proportions(), recent);
        alerts.evaluate(modelId, featureName, psi, ks)
                .ifPresent(alert -> log.info("drift alert for {}/{}: {}", modelId, featureName, alert.explanation()));
    }
}
