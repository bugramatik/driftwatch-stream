package io.driftwatch.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.driftwatch.alert.Alert;
import io.driftwatch.alert.AlertService;
import io.driftwatch.drift.DriftCalculator;
import io.driftwatch.store.BaselineStore;
import io.driftwatch.store.FeatureBaseline;

/**
 * Read side of the service: drift reports per model, alert history and a health
 * summary the Kubernetes probe calls.
 */
@RestController
@RequestMapping("/api")
public class DriftController {

    private static final int RECENT_WINDOW = 500;

    private final BaselineStore store;
    private final AlertService alerts;
    private final DriftCalculator calculator;
    private final JdbcTemplate jdbc;

    public DriftController(BaselineStore store, AlertService alerts, DriftCalculator calculator, JdbcTemplate jdbc) {
        this.store = store;
        this.alerts = alerts;
        this.calculator = calculator;
        this.jdbc = jdbc;
    }

    @GetMapping("/models/{modelId}/drift")
    public DriftReport drift(@PathVariable String modelId) {
        List<FeatureBaseline> baselines = store.baselines(modelId);
        List<FeatureDrift> features = new ArrayList<>(baselines.size());
        double worst = 0.0;
        String worstFeature = null;

        for (FeatureBaseline baseline : baselines) {
            List<Double> recent = store.recentValues(modelId, baseline.featureName(), RECENT_WINDOW);
            if (recent.isEmpty()) {
                continue;
            }
            List<Double> observed = calculator.proportions(baseline.bins(), recent);
            double psi = calculator.psi(baseline.bins(), baseline.proportions(), observed);
            double ks = calculator.ks(baseline.proportions(), observed);
            String severity = calculator.severity(psi);
            features.add(new FeatureDrift(baseline.featureName(), psi, ks, severity, recent.size()));
            if (psi > worst) {
                worst = psi;
                worstFeature = baseline.featureName();
            }
        }

        features.sort((a, b) -> Double.compare(b.psi(), a.psi()));
        return new DriftReport(modelId, worst, worstFeature, features, Instant.now());
    }

    @GetMapping("/models/{modelId}/alerts")
    public List<Alert> alerts(@PathVariable String modelId,
                              @RequestParam(defaultValue = "20") int limit) {
        return alerts.recent(modelId, limit);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("service", "driftwatch-stream");
        body.put("time", Instant.now().toString());
        try {
            Integer models = jdbc.queryForObject("SELECT COUNT(DISTINCT model_id) FROM baselines", Integer.class);
            Integer observations = jdbc.queryForObject("SELECT COUNT(*) FROM observations", Integer.class);
            body.put("models", models == null ? 0 : models);
            body.put("observations", observations == null ? 0 : observations);
        } catch (RuntimeException e) {
            body.put("status", "degraded");
            body.put("error", e.getClass().getSimpleName());
        }
        return body;
    }

    public record FeatureDrift(String feature, double psi, double ks, String severity, int sampleSize) {
    }

    public record DriftReport(String modelId, double driftScore, String topFeature,
                              List<FeatureDrift> features, Instant generatedAt) {
    }
}
