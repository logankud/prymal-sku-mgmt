"""Shared SQL macros for prymal_agent jobs.

`customer_key` has to be byte-identical in every table or the agent database
stops joining, and a join that silently returns nothing is worse than one that
errors. Defining the expression once here - rather than repeating a hash in
each select_query.sql - is what makes that guarantee hold, and the macro is
the only sanctioned way to derive a customer identity.

Usage inside a .sql file:

    SELECT ${CUSTOMER_KEY(customer_email)} AS customer_key, ...

The salt comes from CUSTOMER_KEY_SALT. An unsalted SHA-256 of an email is
reversible by anyone holding a list of candidate addresses - which Prymal has
in Klaviyo, and which anyone obtaining this data could assemble - so the salt
is what makes the key resistant to that, not the hash. The salt must never
rotate: it is the join key for every historical row.
"""
import os
import re
from typing import Optional

# ${CUSTOMER_KEY(col)} / ${CUSTOMER_KEY(alias.col)}
CUSTOMER_KEY_MACRO = re.compile(
    r'\$\{CUSTOMER_KEY\(\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*\)\}')

SALT_ENV_VAR = 'CUSTOMER_KEY_SALT'

# Addresses that are not a person. Each would otherwise hash to one key shared
# by thousands of unrelated orders, producing a fake top-LTV "customer" and
# inflating every repeat-rate and cohort number built on it. Verified against
# production 2026-09-12:
#   unknown@unknown.com          291 Shopify orders - the ETL's fallback for a
#                                blank or invalid email (see models.ShopifyOrder)
#   anonymous-<n>@example.com    128 Shopify orders - Shopify anonymised checkout
#   *@marketplace.amazon.com     every Amazon order - Amazon issues a relay
#                                address that it rotates, so it is not a stable
#                                identity even though it looks like one
#
# Matched against lower(trim(col)). The Amazon rule is anchored to the exact
# relay domain on purpose: 9 real Shopify customers have "amazon" elsewhere in
# their address and must not be discarded.
NON_IDENTITY_SQL = """    WHEN lower(trim({col})) = 'unknown@unknown.com' THEN NULL
    WHEN lower(trim({col})) LIKE 'anonymous-%@example.com' THEN NULL
    WHEN lower(trim({col})) LIKE '%@marketplace.amazon.com' THEN NULL"""


class MissingSaltError(RuntimeError):
    """A query derives a customer key but no salt is configured.

    Falling back to an unsalted hash would produce keys that silently fail to
    join against every row written so far, so this is fatal rather than a
    warning.
    """


def customer_key_sql(column: str, salt: str) -> str:
    """The one true customer_key expression.

    NULL for addresses that do not identify a person, so downstream tables can
    treat "unidentified" as a first-class state instead of inventing a customer.
    """
    non_identity = NON_IDENTITY_SQL.format(col=column)
    return (
        "CASE\n"
        f"    WHEN {column} IS NULL OR trim({column}) = '' THEN NULL\n"
        f"{non_identity}\n"
        f"    ELSE to_hex(sha256(to_utf8(concat('{salt}', lower(trim({column}))))))\n"
        "  END"
    )


def expand_macros(sql: str, salt: Optional[str] = None) -> str:
    """Replace every ${CUSTOMER_KEY(col)} in `sql`.

    `salt` defaults to $CUSTOMER_KEY_SALT. Raises MissingSaltError if the SQL
    needs a salt and none is set.
    """
    if not CUSTOMER_KEY_MACRO.search(sql):
        return sql

    salt = salt if salt is not None else os.getenv(SALT_ENV_VAR)
    if not salt:
        raise MissingSaltError(
            f'This query derives a customer key but {SALT_ENV_VAR} is not set. '
            'Refusing to fall back to an unsalted hash, which would not join '
            'against keys already written.')

    return CUSTOMER_KEY_MACRO.sub(
        lambda m: customer_key_sql(m.group(1), salt), sql)
