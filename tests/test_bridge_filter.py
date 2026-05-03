"""Tests for the per-message filter expression (Sub-feature 2.4).

- evaluate_message_filter() with sandboxed Polars expressions
- Filter passes / blocks based on numeric / string predicates
- Bad expressions raise ValueError (sandbox violations)
- Missing columns fail-closed (drop the message, don't reject the filter)
- Empty filter passes everything
- Bridge WS subscribe accepts filters and rejects bad ones
- Bridge WS forwards filtered messages, drops blocked ones
"""

from __future__ import annotations

import pytest

from resurrector.core.transforms import evaluate_message_filter


class TestEvaluateMessageFilter:
    def test_empty_filter_passes(self):
        assert evaluate_message_filter({"x": 1}, "") is True
        assert evaluate_message_filter({"x": 1}, "   ") is True

    def test_simple_numeric_predicate_pass(self):
        assert evaluate_message_filter({"x": 5}, "pl.col('x') > 3") is True

    def test_simple_numeric_predicate_block(self):
        assert evaluate_message_filter({"x": 1}, "pl.col('x') > 3") is False

    def test_nested_dict_flattens_with_dots(self):
        msg = {
            "linear_acceleration": {"x": 6.0, "y": 0.0, "z": 9.81},
            "angular_velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
        }
        assert evaluate_message_filter(msg, "pl.col('linear_acceleration.x').abs() > 5") is True
        assert evaluate_message_filter(msg, "pl.col('linear_acceleration.y').abs() > 5") is False

    def test_missing_column_fails_closed_for_message(self):
        """If the filter references a column not in this message, drop the message."""
        msg = {"a": 1}
        # Polars will error on missing column 'b'; we should fail-closed (False)
        assert evaluate_message_filter(msg, "pl.col('b') > 0") is False

    def test_string_field_supported(self):
        msg = {"frame_id": "imu_link", "x": 1.0}
        assert evaluate_message_filter(msg, "pl.col('frame_id') == pl.lit('imu_link')") is True
        assert evaluate_message_filter(msg, "pl.col('frame_id') == pl.lit('other')") is False

    def test_compound_predicate(self):
        msg = {"a": 5, "b": 10}
        assert evaluate_message_filter(msg, "(pl.col('a') > 0) & (pl.col('b') < 20)") is True
        assert evaluate_message_filter(msg, "(pl.col('a') > 0) & (pl.col('b') < 5)") is False

    def test_empty_message_fails_closed(self):
        # No fields → can't evaluate → drop
        assert evaluate_message_filter({}, "pl.col('x') > 0") is False


class TestSandboxRejectsBadExpressions:
    def test_rejects_arbitrary_python(self):
        with pytest.raises(ValueError, match="Disallowed name"):
            evaluate_message_filter({"x": 1}, "__import__('os').system('rm -rf /')")

    def test_rejects_unknown_module(self):
        with pytest.raises(ValueError, match="Attribute access not allowed"):
            evaluate_message_filter({"x": 1}, "os.system('whoami')")

    def test_rejects_top_level_call_outside_allowlist(self):
        with pytest.raises(ValueError, match="not in the allowlist"):
            evaluate_message_filter({"x": 1}, "pl.read_csv('/etc/passwd')")

    def test_rejects_syntax_error(self):
        with pytest.raises(ValueError):
            evaluate_message_filter({"x": 1}, "this is not python")


class TestBridgeWSFilterIntegration:
    """Test the filter validation that happens in the WS subscribe handler.

    We validate inline against the same evaluate_message_filter() used
    server-side; full WS round-trip testing requires complex async fixture
    plumbing that's deferred to integration tests.
    """

    def test_valid_filter_validates(self):
        """A valid filter expression doesn't raise during smoke validation."""
        # The bridge's subscribe handler does this exact check
        evaluate_message_filter({}, "pl.col('x') > 5")  # should not raise

    def test_invalid_filter_raises_at_subscribe_time(self):
        """A malicious or malformed filter raises ValueError; bridge should NACK."""
        with pytest.raises(ValueError):
            evaluate_message_filter({}, "__import__('os').system('whoami')")
