package io.driftwatch.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.driftwatch.ingest.PredictionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes rolling feature baselines and recent observations in PostgreSQL.
 *
 * <p>Observations are appended as they arrive from Kafka. Baselines are replaced by the
 * Spark refresh job, which writes bins and expected proportions as JSON arrays.</p>
 */
@Repository
public class BaselineStore {

    private static final Logger log = LoggerFactory.getLogger(BaselineStore.class);
    private static final TypeReference<List<Double>> DOUBLE_LIST = new TypeReference<>() { };

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public BaselineStore(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    private static final RowMapper<FeatureBaseline> BASELINE_MAPPER = (rs, rowNum) -> {
        ObjectMapper local = new ObjectMapper();
        try {
            List<Double> bins = local.readValue(rs.getString("bins"), DOUBLE_LIST);
            List<Double> proportions = local.readValue(rs.getString("proportions"), DOUBLE_LIST);
            Timestamp updated = rs.getTimestamp("updated_at");
            return new FeatureBaseline(
                    rs.getString("model_id"),
                    rs.getString("feature_name"),
                    bins,
                    proportions,
                    updated == null ? Instant.EPOCH : updated.toInstant());
        } catch (Exception e) {
            throw new IllegalStateException("Unreadable baseline for " + rs.getString("feature_name"), e);
        }
    };

    /** Appends one observation so the next baseline refresh can see it. */
    public void record(PredictionEvent event) {
        jdbc.update(
                "INSERT INTO observations (model_id, feature_name, value, predicted_label, observed_at) "
                        + "VALUES (?, ?, ?, ?, ?)",
                event.modelId(),
                event.featureName(),
                event.value(),
                event.predictedLabel(),
                Timestamp.from(event.observedAt()));
    }

    /** All baselines for a model, ordered by feature name. */
    public List<FeatureBaseline> baselines(String modelId) {
        return jdbc.query(
                "SELECT model_id, feature_name, bins, proportions, updated_at FROM baselines "
                        + "WHERE model_id = ? ORDER BY feature_name",
                BASELINE_MAPPER,
                modelId);
    }

    /** The most recent values for one feature, newest first. */
    public List<Double> recentValues(String modelId, String featureName, int limit) {
        int capped = Math.max(1, Math.min(limit, 10_000));
        return jdbc.query(
                "SELECT value FROM observations WHERE model_id = ? AND feature_name = ? "
                        + "ORDER BY observed_at DESC LIMIT ?",
                (rs, rowNum) -> rs.getDouble("value"),
                modelId,
                featureName,
                capped);
    }

    /** Replaces the stored baseline for one feature with freshly computed bins and proportions. */
    public void replaceBaseline(String modelId, String featureName, List<Double> bins, List<Double> proportions) {
        if (bins == null || proportions == null || bins.isEmpty() || bins.size() != proportions.size()) {
            throw new IllegalArgumentException(
                    "bins and proportions must be non-empty and the same length for " + featureName);
        }
        try {
            String binsJson = mapper.writeValueAsString(new ArrayList<>(bins));
            String proportionsJson = mapper.writeValueAsString(new ArrayList<>(proportions));
            jdbc.update(
                    "INSERT INTO baselines (model_id, feature_name, bins, proportions, updated_at) "
                            + "VALUES (?, ?, ?::jsonb, ?::jsonb, ?) "
                            + "ON CONFLICT (model_id, feature_name) DO UPDATE SET "
                            + "bins = EXCLUDED.bins, proportions = EXCLUDED.proportions, "
                            + "updated_at = EXCLUDED.updated_at",
                    modelId,
                    featureName,
                    binsJson,
                    proportionsJson,
                    Timestamp.from(Instant.now()));
        } catch (Exception e) {
            log.error("Could not store baseline for {}/{}: {}", modelId, featureName, e.getMessage());
            throw new IllegalStateException("Could not store baseline for " + featureName, e);
        }
    }

    /** Distinct model ids that have at least one observation, used by the refresh job. */
    public List<String> modelIds() {
        return jdbc.queryForList("SELECT DISTINCT model_id FROM observations ORDER BY model_id", String.class);
    }
}
