"""Reject column names that would put personal data in the agent database.

prymal_agent is exposed to an LLM agent, so the anonymisation rule is a
property of the database rather than of any one job. This is a tripwire for
the obvious mistakes, not a PII classifier: it catches a column called
`customer_email` reaching a config, and it cannot catch personal data hiding
in a free-text column. Review still matters.

A job that genuinely needs one of these columns sets `allow_sensitive` in its
config.yml with a justification, which makes the exception visible in review
rather than silent.
"""
import re
from typing import Iterable, List

# Substrings that are unambiguously personal. Deliberately narrow:
#   - plain "name" is excluded, because it is the product name on almost every
#     table in this database
#   - plain "address" is excluded, because customer_address_state and
#     customer_address_country are sanctioned geo columns
DENY_SUBSTRINGS = (
    'email', 'phone', 'ssn', 'birth', 'dob',
    'street', 'postal', 'zip',
    'first_name', 'last_name', 'full_name', 'customer_name', 'recipient_name',
    'address_line', 'address1', 'address2',
    'shipping_address', 'billing_address',
)


class SensitiveColumnError(ValueError):
    """A config declares a column that looks like personal data."""


def find_sensitive(column_names: Iterable[str]) -> List[str]:
    """Return the column names that trip the denylist."""
    flagged = []
    for name in column_names:
        lowered = re.sub(r'[^a-z0-9_]', '', name.lower())
        if any(token in lowered for token in DENY_SUBSTRINGS):
            flagged.append(name)
    return flagged


def check(column_names: Iterable[str], *, table: str,
          allow_sensitive: bool = False) -> None:
    """Raise unless the columns are clean, or the job opted in explicitly."""
    flagged = find_sensitive(column_names)
    if flagged and not allow_sensitive:
        raise SensitiveColumnError(
            f"Table '{table}' declares column(s) that look like personal data: "
            f"{flagged}. prymal_agent is exposed to an agent, so derive an "
            f"identity with ${{CUSTOMER_KEY(<col>)}} instead. If the column is "
            f"genuinely needed, set allow_sensitive in config.yml with a reason.")
