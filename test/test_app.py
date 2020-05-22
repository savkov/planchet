import glob
import json
import os

import pytest

from planchet.core import WRITE_ONLY, READ_ONLY
from .const import TEST_JOB_NAME


def _make_param_string(params):
    return '&'.join([
        f'{k}={v}' for k, v in params.items()
    ])


@pytest.mark.local
def test_scramble_new(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert response.status_code == 200, response.text


@pytest.mark.local
def test_scramble_restart(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    client.post(f'/serve?job_name={TEST_JOB_NAME}&batch_size=5')
    response = json.loads(client.get(f'/report?job_name={TEST_JOB_NAME}').text)
    assert response['served'] == 5
    job_params['clean_start'] = 'true'
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    response = json.loads(client.get(f'/report?job_name={TEST_JOB_NAME}').text)
    assert response['served'] == 0
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert response.status_code == 200, response.text


@pytest.mark.local
def test_scramble_continue(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    client.post(f'/serve?job_name={TEST_JOB_NAME}&batch_size=5')
    response = json.loads(client.get(f'/report?job_name={TEST_JOB_NAME}').text)
    assert response['served'] == 5
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    assert response.status_code == 400, response.text


@pytest.mark.local
def test_scramble_writing_job(client, metadata_client, csv_items):
    job_params = {
        'job_name': TEST_JOB_NAME,
        'reader_name': '',
        'writer_name': 'CsvWriter',
        'clean_start': 'false',
        'mode': WRITE_ONLY
    }
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata_client
    )
    assert response.status_code == 200, response.text
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=csv_items
    )
    assert response.status_code == 200, response.text
    response = client.post(
        f'/serve?job_name={TEST_JOB_NAME}&batch_size=10',
        json=csv_items
    )
    assert response.status_code == 400, response.text


@pytest.mark.local
def test_scramble_reading_job(client, metadata_client):
    job_params = {
        'job_name': TEST_JOB_NAME,
        'reader_name': 'CsvReader',
        'writer_name': '',
        'clean_start': 'false',
        'mode': READ_ONLY
    }
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata_client
    )
    assert response.status_code == 200, response.text
    n_items = 10
    response = client.post(
        f'/serve?job_name={TEST_JOB_NAME}&batch_size={n_items}'
    )
    assert response.status_code == 200, response.text
    items = json.loads(response.text)
    assert len(items) == n_items
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=items
    )
    assert response.status_code == 400, response.text


@pytest.mark.local
def test_scramble_cont_job(client, metadata_client):
    # start a new job
    job_params = {
        'job_name': TEST_JOB_NAME,
        'reader_name': 'CsvReader',
        'writer_name': 'CsvWriter',
        'clean_start': 'false',
        'cont': False,
        'force_overwrite': 'true'
    }
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata_client
    )
    assert response.status_code == 200, response.text
    # serve some items
    n_served = 10
    n_received = 5
    response = client.post(
        f'/serve?job_name={TEST_JOB_NAME}&batch_size={n_served}'
    )
    assert response.status_code == 200, response.text
    items = json.loads(response.text)
    assert len(items) == n_served
    # send back some of the "processed" items
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=items[:n_received]
    )
    assert response.status_code == 200, response.text
    # make a continuation job
    job_params['cont'] = True
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata_client
    )
    assert response.status_code == 200, response.text
    # generating a report
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert response.status_code == 200, response.text
    report = json.loads(response.text)
    assert report['served'] == 0
    assert report['received'] == n_received


@pytest.mark.local
def test_receive(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    items = json.loads(
        client.post(f'/serve?job_name={TEST_JOB_NAME}&batch_size=5').text
    )
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=items
    )
    assert response.status_code == 200, response.text


@pytest.mark.local
def test_serve_bad_key(client):
    response = client.post(f'/serve?job_name=badjobname&batch_size=5')
    assert response.status_code == 400, response.text


@pytest.mark.local
def test_healthcheck(client):
    response = client.get('/health')
    assert response.status_code == 200, response.text
    assert json.loads(response.text)


@pytest.mark.local
def test_delete(client):
    d = os.path.abspath(os.getcwd())
    input_fp = f'{d}/input_file.csv'
    output_fp = f'{d}/output_file.csv'
    data = 'head1,head2\n' + '\n'.join([f'val{i}1,val{i}2' for i in range(30)])
    with open(input_fp, 'w') as fh:
        fh.write(data)
    client.post(
        '/scramble',
        json={
            'job_name': TEST_JOB_NAME,
            'metadata': {
                'input_file_path': input_fp,
                'output_file_path': output_fp
            }
        }
    )
    client.get(f'/delete?job_name={TEST_JOB_NAME}')
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert not json.loads(response.text), response.text
    os.remove(input_fp)


@pytest.mark.local
def test_mark_errors(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    fake_ids = [1, 2, 3]
    response = client.post(
        f'/mark-errors?job_name={TEST_JOB_NAME}',
        json=fake_ids
    )
    assert response.status_code == 200, response.text


@pytest.mark.local
def test_mark_errors_receive(client, job_params, metadata):
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    items = json.loads(
        client.post(f'/serve?job_name={TEST_JOB_NAME}&batch_size=5').text
    )
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=items
    )
    ids = [id_ for id_, _ in items]
    assert response.status_code == 200, response.text
    response = client.post(
        f'/mark-errors?job_name={TEST_JOB_NAME}',
        json=ids
    )
    assert response.status_code == 400, response.text


@pytest.mark.local
@pytest.mark.parametrize(
    'output', [True, False]
)
def test_clean(client, job_params, metadata, output):
    n_served = 10
    n_received = 5
    param_string = _make_param_string(job_params)
    output_file_path = metadata['output_file_path']
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    items = json.loads(
        client.post(f'/serve?job_name={TEST_JOB_NAME}&'
                    f'batch_size={n_served}').text
    )
    response = client.post(
        f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
        json=items[:n_received]
    )
    assert response.status_code == 200, response.text
    response = client.get(f'/clean?job_name={TEST_JOB_NAME}&output={output}')
    assert response.status_code == 200, response.text
    assert (not os.path.isfile(output_file_path)) is output
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert response.status_code == 200, response.text
    report = json.loads(response.text)
    assert report['served'] == 0
    assert report['received'] == n_received


@pytest.mark.local
@pytest.mark.parametrize(
    'output', [True, False]
)
def test_purge(client, job_params, metadata, output):
    n_served = 10
    n_received = 5
    param_string = _make_param_string(job_params)
    output_file_path = metadata["output_file_path"]
    for i in range(10):
        metadata["output_file_path"] = f'{output_file_path}.{i}'
        client.post(
            f'/scramble?{param_string}',
            json=metadata
        )
        items = json.loads(
            client.post(f'/serve?job_name={TEST_JOB_NAME}&'
                        f'batch_size={n_served}').text
        )
        response = client.post(
            f'/receive?job_name={TEST_JOB_NAME}&overwrite=false',
            json=items[:n_received]
        )
        assert response.status_code == 200, response.text
    response = client.get(f'/purge?output={output}')
    assert response.status_code == 200, response.text
    for fp in glob.glob(f'{output_file_path}*'):
        assert (not os.path.isfile(fp)) is output
        response = client.get(f'/report?job_name={TEST_JOB_NAME}')
        assert response.status_code == 200, response.text
        report = json.loads(response.text)
        assert report == {}


@pytest.mark.local
def test_output_registry(client, job_params, metadata):
    job_params['force_overwrite'] = False
    param_string = _make_param_string(job_params)
    client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    response = client.get(f'/report?job_name={TEST_JOB_NAME}')
    assert response.status_code == 200, response.text
    job_params['job_name'] = 'new_original_job_12342345345'
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    assert response.status_code == 400, 'Output registry did not block creating a new job'
    client.get(f'/purge')
    job_params['job_name'] = 'new_original_job_98693458'
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata
    )
    assert response.status_code == 200, response.text
