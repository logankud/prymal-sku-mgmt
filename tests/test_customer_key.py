"""The customer_key contract.

These tests exist because the failure they guard against is silent: a key
that is derived even slightly differently in one table still looks like a
valid hex string, it just never joins. Nothing downstream would error.
"""
import re

import pytest

from prymal_agent import pii_guard
from prymal_agent.sql_macros import (MissingSaltError, customer_key_sql,
                                     expand_macros)

SALT = 'test-salt-0123456789'


# --------------------------------------------------------------------------
# Macro expansion
# --------------------------------------------------------------------------

def test_macro_is_replaced_with_a_case_expression():
    sql = 'SELECT ${CUSTOMER_KEY(customer_email)} AS customer_key FROM t'
    rendered = expand_macros(sql, salt=SALT)
    assert '${' not in rendered
    assert 'sha256' in rendered
    assert 'customer_email' in rendered


def test_macro_accepts_a_qualified_column():
    rendered = expand_macros('SELECT ${CUSTOMER_KEY(o.email)}', salt=SALT)
    assert 'o.email' in rendered
    assert '${' not in rendered


def test_every_occurrence_is_expanded():
    sql = '${CUSTOMER_KEY(a)} , ${CUSTOMER_KEY(b)}'
    rendered = expand_macros(sql, salt=SALT)
    assert '${' not in rendered
    assert rendered.count('sha256') == 2


def test_sql_without_the_macro_is_untouched_and_needs_no_salt(monkeypatch):
    monkeypatch.delenv('CUSTOMER_KEY_SALT', raising=False)
    sql = 'SELECT 1 FROM t'
    assert expand_macros(sql) == sql


def test_missing_salt_is_fatal_rather_than_falling_back(monkeypatch):
    """An unsalted fallback would produce keys that do not join against the
    rows already written, which is worse than a failed run."""
    monkeypatch.delenv('CUSTOMER_KEY_SALT', raising=False)
    with pytest.raises(MissingSaltError):
        expand_macros('SELECT ${CUSTOMER_KEY(email)}')


def test_salt_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv('CUSTOMER_KEY_SALT', SALT)
    assert SALT in expand_macros('SELECT ${CUSTOMER_KEY(email)}')


def test_the_salt_actually_reaches_the_hash():
    """Guards against the salt being accepted but dropped, which would leave
    the key reversible by dictionary attack while looking correct."""
    rendered = customer_key_sql('email', SALT)
    assert f"concat('{SALT}'" in rendered


def test_different_salts_give_different_expressions():
    assert customer_key_sql('email', 'salt-a') != customer_key_sql('email', 'salt-b')


# --------------------------------------------------------------------------
# Identity semantics
# --------------------------------------------------------------------------

def test_key_is_normalised_before_hashing():
    """Production has both Courtney@prymal.com and courtney@prymal.com, which
    must not be two customers."""
    rendered = customer_key_sql('email', SALT)
    assert 'lower(trim(email))' in rendered


@pytest.mark.parametrize('sentinel', [
    "unknown@unknown.com",          # the ETL's fallback for an invalid email
    "anonymous-%@example.com",      # Shopify anonymised checkout
    "%@marketplace.amazon.com",     # Amazon relay address, rotated by Amazon
])
def test_non_identity_addresses_resolve_to_null(sentinel):
    """Each of these is shared across many unrelated orders; hashing them
    would invent a single fake customer with thousands of orders."""
    rendered = customer_key_sql('email', SALT)
    assert sentinel in rendered
    assert 'THEN NULL' in rendered


def test_blank_and_null_emails_resolve_to_null():
    rendered = customer_key_sql('email', SALT)
    assert 'email IS NULL' in rendered
    assert "trim(email) = ''" in rendered


def test_amazon_rule_is_anchored_to_the_relay_domain():
    """9 real Shopify customers have 'amazon' elsewhere in their address; a
    substring match would silently discard them."""
    rendered = customer_key_sql('email', SALT)
    assert "LIKE '%@marketplace.amazon.com'" in rendered
    assert "LIKE '%amazon%'" not in rendered


# --------------------------------------------------------------------------
# PII guard
# --------------------------------------------------------------------------

@pytest.mark.parametrize('column', [
    'customer_email', 'email', 'customer_email_masked', 'phone_number',
    'customer_name', 'first_name', 'last_name', 'shipping_address',
    'address_line1', 'postal_code', 'zip', 'date_of_birth',
])
def test_personal_columns_are_flagged(column):
    assert pii_guard.find_sensitive([column]) == [column]


@pytest.mark.parametrize('column', [
    'name',                       # product name, on nearly every table here
    'inventory_name',
    'sku_name',
    'customer_address_state',     # sanctioned geo
    'customer_address_country',
    'customer_key',
    'order_cnt',
    'total_fulfillable_quantity',
])
def test_business_columns_are_not_flagged(column):
    assert pii_guard.find_sensitive([column]) == []


def test_check_raises_and_names_the_offending_column():
    with pytest.raises(pii_guard.SensitiveColumnError, match='customer_email'):
        pii_guard.check(['sku', 'customer_email'], table='fct_orders')


def test_check_passes_for_clean_columns():
    pii_guard.check(['sku', 'customer_key'], table='fct_orders')


def test_allow_sensitive_is_an_explicit_opt_out():
    pii_guard.check(['customer_email'], table='t', allow_sensitive=True)


# --------------------------------------------------------------------------
# The expression as a whole
# --------------------------------------------------------------------------

def test_case_expression_is_balanced():
    rendered = customer_key_sql('email', SALT)
    assert rendered.strip().startswith('CASE')
    assert rendered.strip().endswith('END')
    assert len(re.findall(r'\bWHEN\b', rendered)) == 4
    assert rendered.count('ELSE') == 1
