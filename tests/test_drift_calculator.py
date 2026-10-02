"""Tests for the PSI and KS reference implementation shared by the Spark job
and the Java service. The numbers here are the contract: if the Java side and
this file disagree, the baselines written by Spark will not match the scores
computed at ingest time."""

import math

import pytest

from spark.refresh_baselines import build_baseline, psi, ks, severity


BASELINE = [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
EXPECTED = [0.1] * 10


def test_psi_zero_for_identical_distributions():
    observed = [0.1] * 10
    assert psi(EXPECTED, observed) == pytest.approx(0.0, abs=1e-9)


def test_psi_grows_with_shift():
    mild = [0.08, 0.09, 0.10, 0.11, 0.11, 0.11, 0.10, 0.10, 0.10, 0.10]
    heavy = [0.01, 0.02, 0.04, 0.06, 0.09, 0.12, 0.16, 0.18, 0.17, 0.15]
    assert psi(EXPECTED, mild) < psi(EXPECTED, heavy)
    assert psi(EXPECTED, heavy) > 0.25


def test_psi_handles_empty_bins():
    observed = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]
    value = psi(EXPECTED, observed)
    assert math.isfinite(value)
    assert value > 1.0


def test_psi_rejects_mismatched_lengths():
    with pytest.raises(ValueError):
        psi(EXPECTED, [0.5, 0.5])


def test_ks_bounds():
    same = ks(BASELINE, EXPECTED, [0.1] * 10)
    shifted = ks(BASELINE, EXPECTED, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    assert same == pytest.approx(0.0, abs=1e-9)
    assert 0.0 <= shifted <= 1.0
    assert shifted == pytest.approx(1.0, abs=1e-9)


def test_ks_rejects_wrong_bucket_count():
    with pytest.raises(ValueError):
        ks(BASELINE, EXPECTED, [0.2] * 5)


def test_severity_thresholds():
    assert severity(0.05) == "ok"
    assert severity(0.15) == "warn"
    assert severity(0.31) == "alert"


def test_build_baseline_returns_bins_and_proportions():
    values = [float(v) for v in range(100)]
    result = build_baseline(values, bins=10)
    assert len(result["bins"]) == 11
    assert len(result["proportions"]) == 10
    assert sum(result["proportions"]) == pytest.approx(1.0, abs=1e-6)
    assert result["bins"][0] == pytest.approx(0.0)
    assert result["bins"][-1] == pytest.approx(99.0)
