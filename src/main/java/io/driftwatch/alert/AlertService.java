package io.driftwatch.alert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import io.driftwatch.drift.DriftCalculator;

/**
 * Turns drift scores into alerts. An alert is only stored when the PSI crosses the
 * configured warning threshold, and it carries a sentence that names the feature
 * and says how far the distribution moved.
 */
@Service
public class AlertService {

    private final JdbcTemplate jdbc;
    private final DriftCalculator calculator;
    private final double psiWarn;
    private final double psiAlert;

    public AlertService(JdbcTemplate jdbc,
                        DriftCalculator calculator,
                        @Value("${driftwatch.psi-warn:0.1}") double psiWarn,
                        @Value("${driftwatch.psi-alert:0.25}") double psiAlert) {
        this.jdbc = jdbc;
        this.calculator = calculator;
        this.psiWarn = psiWarn;
        this.psiAlert = psiAlert;
    }

    /**
     * Evaluates one feature and stores an alert when it is worth one.
     *
     * @return the stored alert, or empty when the feature is still stable
     */
    public Optional<Alert> evaluate(String modelId, String featureName, double psi, double ks) {
        if (modelId == null || modelId.isBlank() || featureName == null || featureName.isBlank()) {
            throw new IllegalArgumentException("modelId and featureName are required");
        }
        if (psi < psiWarn) {
            return Optional.empty();
        }
        String severity = psi >= psiAlert ? "alert" : "warning";
        String explanation = explain(featureName, psi, ks);
        Instant createdAt = Instant.now();
        Long id = jdbc.queryForObject(
                "INSERT INTO alerts (model_id, feature_name, psi, ks, severity, explanation, created_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                Long.class,
                modelId, featureName, psi, ks, severity, explanation, java.sql.Timestamp.from(createdAt));
        return Optional.of(new Alert(id == null ? 0L : id, modelId, featureName, psi, ks,
                severity, explanation, createdAt));
    }

    /**
     * The most recent alerts for a model, newest first.
     */
    public List<Alert> recent(String modelId, int limit) {
        int capped = Math.max(1, Math.min(limit, 200));
        return jdbc.query(
                "SELECT id, model_id, feature_name, psi, ks, severity, explanation, created_at "
                        + "FROM alerts WHERE model_id = ? ORDER BY created_at DESC LIMIT ?",
                (rs, rowNum) -> mapAlert(rs),
                modelId, capped);
    }

    /**
     * A short sentence a person can read without knowing what PSI means.
     */
    public String explain(String featureName, double psi, double ks) {
        String direction = psi >= psiAlert ? "moved sharply" : "started to move";
        String share = String.format("%.0f%%", Math.min(ks, 1.0) * 100);
        return String.format(
                "The distribution of %s %s since the last baseline: PSI %.2f, and up to %s of the values "
                        + "now fall on a different side of the split than before.",
                featureName, direction, psi, share);
    }

    private Alert mapAlert(ResultSet rs) throws SQLException {
        return new Alert(
                rs.getLong("id"),
                rs.getString("model_id"),
                rs.getString("feature_name"),
                rs.getDouble("psi"),
                rs.getDouble("ks"),
                rs.getString("severity"),
                rs.getString("explanation"),
                rs.getTimestamp("created_at").toInstant());
    }
}
