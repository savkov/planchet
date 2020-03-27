import pytest

from .const import TEST_JOB_NAME


@pytest.mark.local
def test_start_job(planchet_client, live_ledger, metadata):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TEST_JOB_NAME}')
    planchet_client.start_job(TEST_JOB_NAME, metadata, 'CsvReader',
                              'CsvWriter')
    assert live_ledger.get(f'JOB:{TEST_JOB_NAME}')


@pytest.mark.local
def test_delete_job(planchet_client, live_ledger):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.set(f'JOB:{TEST_JOB_NAME}', 'IN_PROGRESS')
    planchet_client.delete_job(TEST_JOB_NAME)
    assert not live_ledger.get(f'JOB:{TEST_JOB_NAME}')


@pytest.mark.local
def test_check(planchet_client):
    foo = planchet_client.check()
    assert foo


@pytest.mark.local
def test_get_job_report(planchet_client, live_ledger, metadata):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TEST_JOB_NAME}')
    planchet_client.start_job(TEST_JOB_NAME, metadata, 'CsvReader',
                              'CsvWriter')
    report = planchet_client.get_job_report(TEST_JOB_NAME)
    assert report


@pytest.mark.local
def test_get(planchet_client, live_ledger, metadata):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TEST_JOB_NAME}')
    planchet_client.start_job(TEST_JOB_NAME, metadata, 'CsvReader',
                              'CsvWriter')
    n_items = 20
    items = planchet_client.get(TEST_JOB_NAME, n_items)
    assert len(items) == n_items


@pytest.mark.local
def test_send(planchet_client, live_ledger, metadata):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TEST_JOB_NAME}')
    planchet_client.start_job(TEST_JOB_NAME, metadata, 'CsvReader',
                              'CsvWriter')
    n_items = 20
    items = planchet_client.get(TEST_JOB_NAME, n_items)
    assert len(items) == n_items
    planchet_client.send(TEST_JOB_NAME, items)
    assert len(list(live_ledger.scan_iter(f'{TEST_JOB_NAME}:*'))) == n_items
