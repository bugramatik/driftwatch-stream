package io.driftwatch.store;

import java.time.Instant;
import java.util.List;

/**
 * One feature's reference distribution: the bin edges and the share of the
 * baseline population that fell into each bin.
 *
 * <p>{@code bins} holds {@code proportions.size() + 1} ascending edges, so bin
 * {@code i} covers {@code [bins[i], bins[i + 1])} and the last bin is closed on
 * the right. The proportions sum to 1.0 (within rounding).
 */
public record FeatureBaseline(
        String modelId,
        String featureName,
        List<Double> bins,
        List<Double> proportions,
        Instant updatedAt) {

    public FeatureBaseline {
        if (modelId == null || modelId.isBlank()) {
            throw new IllegalArgumentException("modelId is required");
        }
        if (featureName == null || featureName.isBlank()) {
            throw new IllegalArgumentException("featureName is required");
        }
        bins = bins == null ? List.of() : List.copyOf(bins);
        proportions = proportions == null ? List.of() : List.copyOf(proportions);
        if (bins.size() != proportions.size() + 1) {
            throw new IllegalArgumentException(
                    "expected " + (proportions.size() + 1) + " bin edges for "
                            + proportions.size() + " proportions, got " + bins.size());
        }
        updatedAt = updatedAt == null ? Instant.now() : updatedAt;
    }

    /** Number of bins in the reference distribution. */
    public int binCount() {
        return proportions.size();
    }

    /** True when the baseline has enough bins to compute a stability index. */
    public boolean isUsable() {
        return proportions.size() >= 2;
    }

    /** Index of the bin that contains {@code value}, clamped to the outer bins. */
    public int binFor(double value) {
        if (proportions.isEmpty()) {
            return -1;
        }
        int last = proportions.size() - 1;
        if (value < bins.get(0)) {
            return 0;
        }
        if (value >= bins.get(last + 1)) {
            return last;
        }
        int lo = 0;
        int hi = last;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (bins.get(mid) <= value) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }
}
