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


REPO = AGENT_DIR.parent.parent


def _runner_job_dirs():
    """Every --job_dir the workflows and backfill scripts hand to the runner."""
    sources = list((REPO / '.github' / 'workflows').glob('*.yml'))
    sources += [REPO / 'src' / 'backfill_all.sh', AGENT_DIR / 'backfill.sh']
    found = set()
    for path in sources:
        found.update(re.findall(r'(?:--job_dir|backfill\.sh)\s+(src/prymal_agent/\S+)', path.read_text()))
    return sorted(found)


@pytest.mark.parametrize('job_dir', _runner_job_dirs())
def test_runner_job_dirs_are_configured(job_dir):
    """The runner raises FileNotFoundError without config.yml and
    select_query.sql; catch that here instead of in a scheduled run."""
    for required in ('config.yml', 'select_query.sql'):
        assert (REPO / job_dir / required).exists(), f'{job_dir} is missing {required}'


@pytest.mark.parametrize('path', sorted(AGENT_DIR.glob('*/select_query.sql')),
                         ids=lambda p: p.parent.name)
def test_select_query_is_embeddable_in_ctas(path):
    """select_query.sql is spliced into CREATE TABLE ... AS <query>; a trailing
    semicolon or ORDER BY would break the statement."""
    body = re.sub(r'--[^\n]*', '', path.read_text()).strip()
    assert not body.endswith(';'), f'{path} must not end with a semicolon'
    assert not re.search(r'\bORDER\s+BY\b[^)]*$', body, re.IGNORECASE), \
        f'{path} ends with ORDER BY, which is meaningless inside CTAS'


@pytest.mark.parametrize('path', _sql_templates(), ids=lambda p: str(p.relative_to(AGENT_DIR)))
def test_no_inline_email_hashing(path):
    """customer_key must come from ${CUSTOMER_KEY(col)}. A hand-rolled hash in
    one file is the failure mode this whole macro exists to prevent: it still
    produces valid-looking hex, it just never joins."""
    text = path.read_text()
    inline = re.search(r'sha256\s*\([^)]*(?:email|customer)', text, re.IGNORECASE)
    assert not inline, (
        f'{path} hashes an email inline; use ${{CUSTOMER_KEY(<column>)}} instead')


def test_every_workflow_that_derives_a_customer_key_passes_the_salt():
    """Without CUSTOMER_KEY_SALT the job now fails loudly rather than writing
    unsalted keys, so a missing secret would break the daily run."""
    macro_jobs = {p.parent.name for p in AGENT_DIR.rglob('*.sql')
                  if 'CUSTOMER_KEY(' in p.read_text()}
    assert macro_jobs, 'expected at least one job to derive a customer key'

    workflow = (REPO / '.github' / 'workflows' / 'agent_reports.yml').read_text()
    for job in macro_jobs:
        step = re.search(rf'- name: Run job - {job}\b.*?(?=\n      - |\Z)',
                         workflow, re.DOTALL)
        assert step, f'no workflow step found for {job}'
        assert 'CUSTOMER_KEY_SALT' in step.group(0), \
            f'{job} derives a customer key but its workflow step has no CUSTOMER_KEY_SALT'


def test_agent_workflow_does_not_call_missing_main():
    """Every `python main.py` step in the agent workflow must run in a directory
    that actually has a main.py; config-only jobs go through the runner."""
    workflow = (AGENT_DIR.parent.parent / '.github' / 'workflows' / 'agent_reports.yml').read_text()
    steps = re.findall(r'working-directory:\s*(\S+)\s*\n\s*run:\s*python3?\s+main\.py', workflow)
    missing = [d for d in steps if not (AGENT_DIR.parent.parent / d / 'main.py').exists()]
    assert not missing, f'workflow steps run main.py in directories without one: {missing}'
