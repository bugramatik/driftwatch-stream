package io.driftwatch.drift;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * Computes the population stability index and the Kolmogorov-Smirnov statistic
 * between a stored baseline and the values observed since the last refresh.
 *
 * The bin edges come from the baseline refresh job, so the expected proportions
 * and the observed proportions are always counted over the same buckets.
 */
@Component
public class DriftCalculator {

    private static final double EPSILON = 1e-6;

    /**
     * Population stability index over the given bin edges.
     *
     * @param bins     ascending bin edges, length n + 1 for n buckets
     * @param expected baseline proportions, length n
     * @param observed current proportions, length n
     * @return the PSI, or 0 when both distributions are identical
     */
    public double psi(List<Double> bins, List<Double> expected, List<Double> observed) {
        if (bins == null || expected == null || observed == null) {
            throw new IllegalArgumentException("bins, expected and observed are required");
        }
        int buckets = bins.size() - 1;
        if (buckets < 1) {
            throw new IllegalArgumentException("at least two bin edges are required");
        }
        if (expected.size() != buckets || observed.size() != buckets) {
            throw new IllegalArgumentException(
                    "expected and observed must hold one proportion per bucket (" + buckets + ")");
        }

        double total = 0.0;
        for (int i = 0; i < buckets; i++) {
            double e = Math.max(expected.get(i), EPSILON);
            double o = Math.max(observed.get(i), EPSILON);
            total += (o - e) * Math.log(o / e);
        }
        return total;
    }

    /**
     * Two-sample Kolmogorov-Smirnov statistic between the baseline sample and the
     * recent sample. Both samples are sorted in place on copies, so callers keep
     * their lists untouched.
     */
    public double ks(List<Double> baseline, List<Double> recent) {
        if (baseline == null || recent == null || baseline.isEmpty() || recent.isEmpty()) {
            throw new IllegalArgumentException("both samples must hold at least one value");
        }
        List<Double> a = new ArrayList<>(baseline);
        List<Double> b = new ArrayList<>(recent);
        Collections.sort(a);
        Collections.sort(b);

        int i = 0;
        int j = 0;
        double maxGap = 0.0;
        while (i < a.size() && j < b.size()) {
            double value = Math.min(a.get(i), b.get(j));
            while (i < a.size() && a.get(i) <= value) {
                i++;
            }
            while (j < b.size() && b.get(j) <= value) {
                j++;
            }
            double cdfA = (double) i / a.size();
            double cdfB = (double) j / b.size();
            maxGap = Math.max(maxGap, Math.abs(cdfA - cdfB));
        }
        return maxGap;
    }

    /**
     * Buckets the recent values over the baseline edges and returns one proportion
     * per bucket. Values below the first edge and above the last edge land in the
     * outer buckets, which is what the refresh job does as well.
     */
    public List<Double> proportions(List<Double> bins, List<Double> values) {
        int buckets = bins.size() - 1;
        double[] counts = new double[buckets];
        for (double value : values) {
            int index = bucketOf(bins, value);
            counts[index] += 1.0;
        }
        List<Double> result = new ArrayList<>(buckets);
        double total = values.isEmpty() ? 1.0 : values.size();
        for (double count : counts) {
            result.add(count / total);
        }
        return result;
    }

    private int bucketOf(List<Double> bins, double value) {
        int buckets = bins.size() - 1;
        if (value <= bins.get(0)) {
            return 0;
        }
        for (int i = 1; i < buckets; i++) {
            if (value <= bins.get(i)) {
                return i - 1;
            }
        }
        return buckets - 1;
    }

    /**
     * Maps a PSI to a severity label. The thresholds match the ones the service
     * reads from configuration: below warn is stable, below alert is a warning.
     */
    public String severity(double psi) {
        if (psi < 0.1) {
            return "stable";
        }
        if (psi < 0.25) {
            return "warning";
        }
        return "alert";
    }
}
