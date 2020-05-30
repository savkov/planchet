import json

import pytest

from .const import TOKEN_TEST_JOB_NAME

TOKEN = 'test-random-token'


@pytest.mark.local
def test_start_job(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(
        job_name=TOKEN_TEST_JOB_NAME, metadata=metadata_client,
        reader_name='CsvReader', writer_name='CsvWriter', token=TOKEN)
    assert response.status_code == 200, response.text
    response = live_ledger.get(f'JOB:{TOKEN_TEST_JOB_NAME}')
    assert response is not None, response.text


@pytest.mark.local
def test_delete_job(planchet_client, live_ledger):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.set(f'JOB:{TOKEN_TEST_JOB_NAME}', 'IN_PROGRESS')
    planchet_client.delete_job(job_name=TOKEN_TEST_JOB_NAME, token=TOKEN)
    assert not live_ledger.get(f'JOB:{TOKEN_TEST_JOB_NAME}')


@pytest.mark.local
def test_check(planchet_client):
    response = planchet_client.check()
    assert response


@pytest.mark.local
def test_get_job_report(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(TOKEN_TEST_JOB_NAME, metadata_client,
                                         'CsvReader', 'CsvWriter')
    assert response.status_code == 200, response
    report = planchet_client.get_job_report(TOKEN_TEST_JOB_NAME)
    assert report, report


@pytest.mark.local
def test_get(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(
        job_name=TOKEN_TEST_JOB_NAME, metadata=metadata_client,
        reader_name='CsvReader', writer_name='CsvWriter', token=TOKEN)
    assert response.status_code == 200, response
    n_items = 20
    items = planchet_client.get(job_name=TOKEN_TEST_JOB_NAME, n_items=n_items,
                                token=TOKEN)
    assert len(items) == n_items, items


@pytest.mark.local
def test_send(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(
        job_name=TOKEN_TEST_JOB_NAME, metadata=metadata_client,
        reader_name='CsvReader', writer_name='CsvWriter', token=TOKEN)
    assert response.status_code == 200, response
    n_items = 20
    items = planchet_client.get(job_name=TOKEN_TEST_JOB_NAME, n_items=n_items,
                                token=TOKEN)
    assert len(items) == n_items, items
    planchet_client.send(job_name=TOKEN_TEST_JOB_NAME, items=items, token=TOKEN)
    scanned_items = list(live_ledger.scan_iter(f'{TOKEN_TEST_JOB_NAME}:*'))
    assert len(scanned_items) == n_items, scanned_items


@pytest.mark.local
def test_mark_errors(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(
        job_name=TOKEN_TEST_JOB_NAME, metadata=metadata_client,
        reader_name='CsvReader', writer_name='CsvWriter', token=TOKEN)
    assert response.status_code == 200, response
    fake_ids = [1, 2, 3]
    response = planchet_client.mark_errors(job_name=TOKEN_TEST_JOB_NAME,
                                           ids=fake_ids, token=TOKEN)
    assert response.status_code == 200


@pytest.mark.local
def test_mark_errors_received(planchet_client, live_ledger, metadata_client):
    assert planchet_client.check()['Redis status'] == 'Online', 'Redis offline'
    live_ledger.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    response = planchet_client.start_job(
        job_name=TOKEN_TEST_JOB_NAME, metadata=metadata_client,
        reader_name='CsvReader', writer_name='CsvWriter', token=TOKEN)
    assert response.status_code == 200, response
    n_items = 20
    items = planchet_client.get(job_name=TOKEN_TEST_JOB_NAME, n_items=n_items,
                                token=TOKEN)
    assert len(items) == n_items, items
    planchet_client.send(job_name=TOKEN_TEST_JOB_NAME, items=items, token=TOKEN)
    ids = [id_ for id_, _ in items]
    response = planchet_client.mark_errors(job_name=TOKEN_TEST_JOB_NAME,
                                           ids=ids, token=TOKEN)
    assert response.status_code == 400
