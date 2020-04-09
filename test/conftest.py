import os

from fastapi.testclient import TestClient
import pytest
import fakeredis

from planchet.core import Job, READ_ONLY, WRITE_ONLY
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
def csv_items():
    return [
        (1, ['val1', 'val2']),
        (2, ['val1', 'val2']),
        (3, ['val1', 'val2']),
    ]


@pytest.fixture(scope='function')
def client():
    assert LEDGER is not None, f'Cannot connect to redis during testing: ' \
                               f'{REDIS_HOST}:{REDIS_PORT} using password {bool(REDIS_PWD)}'
    yield TestClient(app)
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for k in LEDGER.scan_iter(f'{TEST_JOB_NAME}:*'):
        LEDGER.delete(k)


@pytest.fixture(scope='function')
def ledger():
    r = fakeredis.FakeRedis()
    yield r
    # clean up
    for k in r.scan_iter(f'*'):
        r.delete(k)


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
