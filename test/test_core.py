import json

from pydantic.typing import NoneType

from .const import CSV_SIZE

from typing import Dict
import pytest

from planchet import Job
from planchet.core import COMPLETE, IN_PROGRESS, RECEIVED, SERVED


@pytest.mark.parametrize('batch_size', [1, 2, 5, 10, 13, 30, 32])
def test_serve_batch(reader, writer, ledger, batch_size):
    job = Job('somejob', reader, writer, ledger)
    items = job.serve(batch_size)
    n_items = batch_size if batch_size < 30 else 30
    assert len(items) == n_items


@pytest.mark.parametrize('pre_served_size', [1, 2, 5, 10, 13, 30])
def test_serve_continuation(reader, writer, ledger, pre_served_size):
    job = Job('somejob', reader, writer, ledger)
    for i in range(pre_served_size):
        ledger.set(job.ledger_id(i), 'fake value')
    items = job.serve(50)
    n_items = CSV_SIZE - pre_served_size
    assert len(items) == n_items


def test_status(reader, writer, ledger):
    job = Job('somejob', reader, writer, ledger)
    assert job.status == IN_PROGRESS
    items = job.serve(5)
    assert job.status == IN_PROGRESS
    items.extend(job.serve(100))
    job.receive(items, True)
    assert job.status == COMPLETE


def test_restore_records(reader, writer, ledger):
    job_name: str = 'somejob'
    n_skips: int = 10
    for i in range(n_skips):
        ledger.set(f'{job_name}:{i}', RECEIVED)
    ledger.set(f'{job_name}:1', SERVED)
    job: Job = Job(job_name, reader, writer, ledger)
    assert len(job.received) == n_skips - 1
    assert len(job.served) == n_skips
    items = job.serve(100)
    assert len(items) == CSV_SIZE - n_skips


def test_restore_job(reader, writer, ledger):
    job_name: str = 'testjob'
    job_key: str = 'JOB:testjob'
    metadata: Dict = {
        'input_file_path': reader.file_path,
        'output_file_path': writer.file_path
    }
    value = json.dumps({
        'metadata': metadata,
        'reader_name': type(reader).__name__,
        'writer_name': type(writer).__name__
    })
    ledger.set(job_key, value)
    n_skips: int = 10
    for i in range(n_skips):
        ledger.set(f'{job_name}:{i}', RECEIVED)
    ledger.set(f'{job_name}:1', SERVED)
    job: Job = Job.restore_job(job_name, job_key, ledger)
    assert len(job.received) == n_skips - 1
    assert len(job.served) == n_skips
    records = job.serve(100)
    assert len(records) == CSV_SIZE - n_skips


def test_restore_unknown_job(ledger):
    job_name: str = 'testjob'
    job_key: str = 'JOB:testjob'
    job: NoneType = Job.restore_job(job_name, job_key, ledger)
    assert job is None


def test_restart(reader, writer, ledger):
    jobname = 'somejob'
    n_skips = 10
    for i in range(n_skips):
        ledger.set(f'{jobname}:{i}', RECEIVED)
    assert len(list(ledger.scan_iter(f'{jobname}*'))) == n_skips
    job = Job(jobname, reader, writer, ledger)
    job.restart()
    assert not len(list(ledger.scan_iter(f'{jobname}*')))
    assert not job.served
    assert not job.received


def test_receive(job):
    jobname = job.name
    ledger = job.ledger
    items = job.serve(CSV_SIZE)
    n_processed = 10
    job.receive(items[:n_processed], False)
    received = [x for x in ledger.scan_iter(f'{jobname}*')
                if ledger.get(x).decode('utf8') == RECEIVED]
    served = [x for x in ledger.scan_iter(f'{jobname}*')
              if ledger.get(x).decode('utf8') == SERVED]
    active = job.served - job.received

    assert len(received) == n_processed
    assert len(received) == len(job.received)
    assert len(served) == CSV_SIZE - n_processed
    assert set(int(x.decode('utf8').rsplit(':')[1]) for x in served) == active


def test_stats(job):
    n_served = 10
    n_received = 2
    served = job.serve(n_served)
    job.receive(served[:n_received], False)
    job.receive(served[:n_received], True)
    stats = job.stats
    assert stats['served'] == n_served
    assert stats['received'] == 2
    assert stats['status'] == IN_PROGRESS
