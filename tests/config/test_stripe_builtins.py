"""Tests for STRIPE_BUILTINS and get_stripe_builtins in config module."""

import pytest

from config import STRIPE_BUILTINS, get_stripe_builtins


class TestStripeBuiltins:
    """Tests for Stripe-specific built-in annotation definitions."""

    def test_stripe_builtins_is_dict(self):
        assert isinstance(STRIPE_BUILTINS, dict)

    def test_get_stripe_builtins_returns_copy(self):
        """get_stripe_builtins should return a copy, not the original."""
        result = get_stripe_builtins()
        assert result == STRIPE_BUILTINS
        # Mutating the returned dict should not affect the original
        result["test_key"] = "test_value"
        assert "test_key" not in STRIPE_BUILTINS

    def test_card_number_present(self):
        assert "stripe_card_number" in STRIPE_BUILTINS

    def test_card_cvc_present(self):
        assert "stripe_card_cvc" in STRIPE_BUILTINS

    def test_card_exp_present(self):
        assert "stripe_card_exp" in STRIPE_BUILTINS

    def test_customer_email_present(self):
        assert "stripe_customer_email" in STRIPE_BUILTINS

    def test_customer_name_present(self):
        assert "stripe_customer_name" in STRIPE_BUILTINS

    def test_customer_phone_present(self):
        assert "stripe_customer_phone" in STRIPE_BUILTINS

    def test_customer_address_present(self):
        assert "stripe_customer_address" in STRIPE_BUILTINS

    def test_card_fields_are_financial(self):
        for key in ("stripe_card_number", "stripe_card_cvc", "stripe_card_exp"):
            assert STRIPE_BUILTINS[key]["classification"] == "FINANCIAL"

    def test_customer_fields_are_pii(self):
        for key in ("stripe_customer_email", "stripe_customer_name",
                     "stripe_customer_phone", "stripe_customer_address"):
            assert STRIPE_BUILTINS[key]["classification"] == "PII"

    def test_card_number_has_encrypted_and_tokenized(self):
        ann = STRIPE_BUILTINS["stripe_card_number"]["annotations"]
        assert "encrypted_at_rest" in ann
        assert "tokenized" in ann

    def test_customer_email_has_pii_and_gdpr(self):
        ann = STRIPE_BUILTINS["stripe_customer_email"]["annotations"]
        assert "pii_field" in ann
        assert "gdpr_erasable" in ann

    def test_all_entries_have_required_keys(self):
        required_keys = {"description", "field_pattern", "classification", "annotations", "propagation"}
        for name, defn in STRIPE_BUILTINS.items():
            for key in required_keys:
                assert key in defn, f"Missing key '{key}' in {name}"

    def test_all_propagation_rules_have_required_keys(self):
        prop_keys = {"pact_assertion_type", "arbiter_tier_behavior", "baton_masking_rule", "sentinel_severity"}
        for name, defn in STRIPE_BUILTINS.items():
            for key in prop_keys:
                assert key in defn["propagation"], f"Missing propagation key '{key}' in {name}"

    def test_card_number_sentinel_severity_is_critical(self):
        assert STRIPE_BUILTINS["stripe_card_number"]["propagation"]["sentinel_severity"] == "critical"

    def test_card_number_baton_masking_is_full(self):
        assert STRIPE_BUILTINS["stripe_card_number"]["propagation"]["baton_masking_rule"] == "full_mask"

    def test_customer_email_baton_masking_is_partial(self):
        assert STRIPE_BUILTINS["stripe_customer_email"]["propagation"]["baton_masking_rule"] == "partial_mask"
