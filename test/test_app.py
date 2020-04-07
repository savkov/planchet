import json
import os

import pytest

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
def test_scramble_dumping_job(client, metadata_client):
    job_params = {
        'job_name': TEST_JOB_NAME,
        'reader_name': '',
        'writer_name': 'CsvWriter',
        'clean_start': 'false'
    }
    param_string = _make_param_string(job_params)
    response = client.post(
        f'/scramble?{param_string}',
        json=metadata_client
    )
    assert response.status_code == 200, response.text


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
