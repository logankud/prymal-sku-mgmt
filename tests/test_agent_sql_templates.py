"""Guard rails for the SQL templates under src/prymal_agent/.

Every template is rendered by string substitution of ${VAR} placeholders, so
two classes of mistake slip through silently until Athena runs the statement:
a literal bucket name that ignores S3_BUCKET_NAME, and a placeholder that no
renderer knows how to fill. Both are caught here without touching AWS.
"""
import re
from pathlib import Path

import pytest

AGENT_DIR = Path(__file__).resolve().parent.parent / 'src' / 'prymal_agent'

# Placeholders that the generic runner (runner.py) and the standalone sub-job
# scripts know how to substitute.
KNOWN_PLACEHOLDERS = {
    'S3_BUCKET', 'DATABASE', 'AGENT_DATABASE', 'COLUMNS', 'TABLE_NAME',
    'TABLE_DESCRIPTION', 'RUN_DATE', 'RUN_ID', 'PARTITION_COLUMN',
    'SELECT_QUERY', 'STAGING_LOCATION',
}

PLACEHOLDER = re.compile(r'\$\{([A-Z_]+)\}')


def _sql_templates():
    # The *_OLD directory is a retired job kept for reference only.
    return sorted(p for p in AGENT_DIR.rglob('*.sql')
                  if not any(part.endswith('_OLD') for part in p.parts))


@pytest.mark.parametrize('path', _sql_templates(), ids=lambda p: str(p.relative_to(AGENT_DIR)))
def test_no_hardcoded_bucket(path):
    text = path.read_text()
    for match in re.finditer(r"s3://([^/'\s]+)/", text):
        assert match.group(1) == '${S3_BUCKET}', (
            f'{path.name} hardcodes bucket {match.group(1)!r}; use s3://${{S3_BUCKET}}/')


@pytest.mark.parametrize('path', _sql_templates(), ids=lambda p: str(p.relative_to(AGENT_DIR)))
def test_only_known_placeholders(path):
    unknown = set(PLACEHOLDER.findall(path.read_text())) - KNOWN_PLACEHOLDERS
    assert not unknown, f'{path.name} uses placeholders nobody renders: {sorted(unknown)}'


def test_agent_workflow_does_not_call_missing_main():
    """Every `python main.py` step in the agent workflow must run in a directory
    that actually has a main.py; config-only jobs go through the runner."""
    workflow = (AGENT_DIR.parent.parent / '.github' / 'workflows' / 'agent_reports.yml').read_text()
    steps = re.findall(r'working-directory:\s*(\S+)\s*\n\s*run:\s*python3?\s+main\.py', workflow)
    missing = [d for d in steps if not (AGENT_DIR.parent.parent / d / 'main.py').exists()]
    assert not missing, f'workflow steps run main.py in directories without one: {missing}'
