import os


from fastapi.testclient import TestClient
import pytest
import fakeredis

from planchet import Job
from planchet.io import CsvReader, CsvWriter
from .const import TEST_JOB_NAME
from app import app, LEDGER


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
    d = os.path.abspath(os.getcwd())
    fp = f'{d}/input_file.csv'
    with open(fp, 'w') as fh:
        fh.write(data)
    yield fp
    os.remove(fp)


@pytest.fixture()
def output_fp():
    d = os.path.abspath(os.getcwd())
    fp = f'{d}/output_file.csv'
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


@pytest.fixture(scope='function')
def client():
    yield TestClient(app)
    LEDGER.delete(f'JOB:{TEST_JOB_NAME}')
    for k in LEDGER.scan_iter(f'{TEST_JOB_NAME}:*'):
        LEDGER.delete(k)


@pytest.fixture()
def ledger():
    return fakeredis.FakeRedis()


@pytest.fixture()
def reader():
    fp = 'tempfile.csv'
    data = 'head1,head2\n' + '\n'.join([f'val{i}1,val{i}2' for i in range(30)])
    with open('tempfile.csv', 'w') as fh:
        fh.write(data)
    metadata = {
        'input_file_path': fp
    }
    yield CsvReader(metadata)
    os.remove(fp)


@pytest.fixture()
def writer():
    fp = 'tempfile-out.csv'
    metadata = {'output_file_path': fp}
    yield CsvWriter(metadata)
    if os.path.exists(fp):
        os.remove(fp)


@pytest.fixture(scope='function')
def job(reader, writer, ledger):
    jobname = 'somejob'
    return Job(jobname, reader, writer, ledger)
