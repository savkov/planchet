import os
import random

from fastapi.testclient import TestClient
import pytest
import fakeredis

from planchet.core import Job, READ_ONLY, WRITE_ONLY
from planchet.io import CsvReader, CsvWriter
from planchet.client import PlanchetClient
from planchet.config import REDIS_HOST, REDIS_PORT, REDIS_PWD, MASTER_TOKEN
from .const import (
    TEST_JOB_NAME, TOKEN_TEST_JOB_NAME, PLANCHET_HOST, PLANCHET_PORT
)
from app import app, LEDGER


@pytest.fixture(scope='session')
def planchet_client():
    url = f'http://{PLANCHET_HOST}:{PLANCHET_PORT}'
    client = PlanchetClient(url)
    yield client
    client.purge_server(master_token=MASTER_TOKEN)


@pytest.fixture()
def job_params():
    return {
        'job_name': TEST_JOB_NAME,
        'reader_name': 'CsvReader',
        'writer_name': 'CsvWriter',
        'clean_start': 'false',
        'force_overwrite': 'true'
    }


@pytest.fixture()
def token_job_params():
    return {
        'job_name': f'token-{TEST_JOB_NAME}',
        'reader_name': 'CsvReader',
        'writer_name': 'CsvWriter',
        'clean_start': 'false',
        'force_overwrite': 'true',
        'token': 'some-token'
    }


@pytest.fixture()
def input_fp(data):
    fp = 'input_file.csv'
    with open(fp, 'w') as fh:
        fh.write(data)
    yield fp
    os.remove(fp)


@pytest.fixture()
def input_fp_client(data):
    fp = '/data/client_input_file.csv'
    with open(fp, 'w') as fh:
        fh.write(data)
    yield fp
    os.remove(fp)


@pytest.fixture(scope='function')
def output_fp():
    fp = f'output_file.{str(random.randint(0, 1000))}.csv'
    yield fp
    try:
        os.remove(fp)
    except FileNotFoundError:
        pass


@pytest.fixture()
def output_fp_client():
    fp = f'/data/client_output_file.{str(random.randint(0, 1000))}.csv'
    yield fp
    try:
        os.remove(fp)
    except FileNotFoundError:
        pass


@pytest.fixture()
def data():
    return 'head1,head2\n' + '\n'.join([f'val{i}1,val{i}2' for i in range(30)])


@pytest.fixture()
def metadata(input_fp, output_fp):
    return {'input_file_path': input_fp, 'output_file_path': output_fp}


@pytest.fixture()
def metadata_client(input_fp_client, output_fp_client):
    return {
        'input_file_path': input_fp_client,
        'output_file_path': output_fp_client
    }


@pytest.fixture(scope='function')
def csv_items():
    return [
        (1, ['val1', 'val2']),
        (2, ['val1', 'val2']),
        (3, ['val1', 'val2']),
    ]


@pytest.fixture(scope='function')
def client():
    assert LEDGER is not None, f'Cannot connect to redis during testing: ' \
                               f'{REDIS_HOST}:{REDIS_PORT} using password ' \
                               f'{bool(REDIS_PWD)}'
    yield TestClient(app)
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for k in list(LEDGER.scan_iter(f'{TEST_JOB_NAME}:*')):
        LEDGER.delete(k)
    LEDGER.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    for k in list(LEDGER.scan_iter(f'{TOKEN_TEST_JOB_NAME}:*')):
        LEDGER.delete(k)


@pytest.fixture(scope='function')
def ledger():
    r = fakeredis.FakeRedis()
    yield r
    # clean up
    for k in list(r.scan_iter(f'*')):
        r.delete(k)


@pytest.fixture()
def live_ledger():
    yield LEDGER
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for key in list(LEDGER.scan_iter(f'{TEST_JOB_NAME}:*')):
        LEDGER.delete(key)
    LEDGER.delete(f'JOB:{TOKEN_TEST_JOB_NAME}')
    for k in list(LEDGER.scan_iter(f'{TOKEN_TEST_JOB_NAME}:*')):
        LEDGER.delete(k)


@pytest.fixture()
def reader(input_fp):
    metadata = {
        'input_file_path': input_fp
    }
    return CsvReader(metadata)


@pytest.fixture(scope='function')
def writer(output_fp):
    metadata = {'output_file_path': output_fp}
    return CsvWriter(metadata)


@pytest.fixture(scope='function')
def job(reader, writer, ledger):
    job_name = 'somejob'
    yield Job(job_name, reader, writer, ledger)


@pytest.fixture(scope='function')
def reading_job(reader, ledger):
    job_name = 'reading-job'
    yield Job(job_name, reader, None, ledger, READ_ONLY)


@pytest.fixture(scope='function')
def writing_job(writer, ledger):
    job_name = 'writing-job'
    yield Job(job_name, None, writer, ledger, WRITE_ONLY)
