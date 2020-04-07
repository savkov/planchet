import os

from fastapi.testclient import TestClient
import pytest
import fakeredis

from planchet.core import Job
from planchet.io import CsvReader, CsvWriter
from planchet.client import PlanchetClient
from planchet.config import REDIS_HOST, REDIS_PORT, REDIS_PWD
from .const import TEST_JOB_NAME, PLANCHET_HOST, PLANCHET_PORT
from app import app, LEDGER


@pytest.fixture(scope='session')
def planchet_client():
    url = f'http://{PLANCHET_HOST}:{PLANCHET_PORT}'
    return PlanchetClient(url)


@pytest.fixture()
def job_params():
    return {
        'job_name': TEST_JOB_NAME,
        'reader_name': 'CsvReader',
        'writer_name': 'CsvWriter',
        'clean_start': 'false'
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


@pytest.fixture()
def output_fp():
    fp = 'output_file.csv'
    yield fp
    try:
        os.remove(fp)
    except FileNotFoundError:
        pass


@pytest.fixture()
def output_fp_client():
    fp = '/data/client_output_file.csv'
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
    return {'input_file_path': input_fp_client, 'output_file_path': output_fp_client}


@pytest.fixture(scope='function')
def client():
    assert LEDGER is not None, f'Cannot connect to redis during testing: ' \
                               f'{REDIS_HOST}:{REDIS_PORT} using password {bool(REDIS_PWD)}'
    yield TestClient(app)
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for k in LEDGER.scan_iter(f'{TEST_JOB_NAME}:*'):
        LEDGER.delete(k)


@pytest.fixture()
def ledger():
    return fakeredis.FakeRedis()


@pytest.fixture()
def live_ledger():
    yield LEDGER
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for key in LEDGER.scan_iter(f'{TEST_JOB_NAME}:*'):
        LEDGER.delete(key)


@pytest.fixture()
def reader(input_fp):
    metadata = {
        'input_file_path': input_fp
    }
    return CsvReader(metadata)


@pytest.fixture()
def writer(output_fp):
    metadata = {'output_file_path': output_fp}
    return CsvWriter(metadata)


@pytest.fixture(scope='function')
def job(reader, writer, ledger):
    jobname = 'somejob'
    return Job(jobname, reader, writer, ledger)
