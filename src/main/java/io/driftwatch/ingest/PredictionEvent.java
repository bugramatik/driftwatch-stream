package io.driftwatch.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;

/**
 * One prediction event as it arrives from Kafka. Each event carries a single
 * feature value that was used for a prediction, so a model with ten features
 * produces ten events per prediction.
 */
public record PredictionEvent(
        String modelId,
        String featureName,
        double value,
        String predictedLabel,
        Instant observedAt) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public PredictionEvent {
        if (modelId == null || modelId.isBlank()) {
            throw new IllegalArgumentException("modelId is required");
        }
        if (featureName == null || featureName.isBlank()) {
            throw new IllegalArgumentException("featureName is required");
        }
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("value must be finite for feature " + featureName);
        }
        if (observedAt == null) {
            observedAt = Instant.now();
        }
    }

    /**
     * Parses the JSON payload produced by the scoring services. Accepts both
     * {@code model_id} and {@code modelId} spellings so older producers keep working.
     */
    public static PredictionEvent fromJson(String json) throws IOException {
        JsonNode node = MAPPER.readTree(json);
        String modelId = text(node, "model_id", "modelId");
        String featureName = text(node, "feature_name", "featureName");
        JsonNode valueNode = node.has("value") ? node.get("value") : node.get("feature_value");
        if (valueNode == null || !valueNode.isNumber()) {
            throw new IOException("event has no numeric value: " + json);
        }
        String label = text(node, "predicted_label", "predictedLabel");
        Instant observedAt = Instant.now();
        String ts = text(node, "observed_at", "observedAt");
        if (ts != null && !ts.isBlank()) {
            observedAt = Instant.parse(ts);
        }
        return new PredictionEvent(modelId, featureName, valueNode.asDouble(), label, observedAt);
    }

    private static String text(JsonNode node, String first, String second) {
        JsonNode value = node.has(first) ? node.get(first) : node.get(second);
        return value == null || value.isNull() ? null : value.asText();
    }
}
