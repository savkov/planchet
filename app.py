import json
import sys
from typing import List, Callable, Dict, Tuple, Union

from fastapi import FastAPI, HTTPException
from redis import Redis
from redis.exceptions import ConnectionError

from planchet.core import Job, COMPLETE
from planchet.config import REDIS_HOST, REDIS_PORT, REDIS_PWD, MAX_PACKAGE_SIZE
import planchet.io as io

app = FastAPI()


def _load_jobs(ledger) -> Dict:
    jobs: Dict = {}
    for job_key in ledger.scan_iter(f'JOB:*'):
        job_name: str = job_key.decode('utf8').split(':', 1)[1]
        jobs[job_name] = Job.restore_job(job_name, job_key, ledger)
    return jobs


def _job_id(name):
    return f'JOB:{name}'


try:
    LEDGER: Redis = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD)
    JOB_LOG: Dict = _load_jobs(LEDGER)
except ConnectionError:
    # There is no Redis connection; this fixes test imports
    # noinspection PyTypeChecker
    LEDGER = None  # type: ignore
    JON_LOG = {}


@app.post("/scramble")
def scramble(job_name: str, metadata: Dict, reader_name: str,
             writer_name: str, clean_start: bool = False):
    reader: Callable = getattr(io, reader_name)(metadata)
    writer: Callable = getattr(io, writer_name)(metadata)
    new_job: Job = Job(job_name, reader, writer, LEDGER)
    # clean ledger before starting
    if clean_start:
        new_job.restart()
    # pick up where you left off
    elif LEDGER.get(f'JOB:{job_name}'):
        msg = f'Job {job_name} already exists.'
        raise HTTPException(status_code=400, detail=msg)
    JOB_LOG[job_name] = new_job
    LEDGER.set(f'JOB:{job_name}', json.dumps({
        'metadata': metadata, 'reader_name': reader_name,
        'writer_name': writer_name}))


@app.post("/serve")
def serve(job_name: str, batch_size: int = 100) -> List:
    try:
        job = JOB_LOG[job_name]
    except KeyError:
        active = LEDGER.get(job_name)
        no_active_msg = f'No active job: {job_name}'
        no_known_msg = f'No known job: {job_name}'
        msg = no_active_msg if active else no_known_msg
        raise HTTPException(status_code=400, detail=msg)
    return job.serve(batch_size)


@app.post("/receive")
def receive(job_name: str, items: List[Tuple[int, Union[Dict, List]]],
            overwrite: bool):
    size = sys.getsizeof(items)
    if size > MAX_PACKAGE_SIZE:
        msg = f'In-memory paylod must be less than {MAX_PACKAGE_SIZE}; ' \
              f'currentsize is {size}.'
        raise HTTPException(status_code=413, detail=msg)
    job = JOB_LOG[job_name]
    job.receive(items, overwrite)
    if job.status == COMPLETE:
        LEDGER.set(_job_id(job_name), COMPLETE)


@app.get('/delete')
def delete(job_name: str):
    try:
        del JOB_LOG[job_name]
    except KeyError:
        pass
    LEDGER.delete(f'JOB:{job_name}')
    for record in LEDGER.scan_iter(f'{job_name}:*'):
        LEDGER.delete(record)


@app.get("/report")
def report(job_name: str) -> Dict:
    try:
        return JOB_LOG[job_name].stats
    except KeyError:
        return {}


@app.get("/health")
def healthcheck() -> Dict:
    try:
        LEDGER.ping()
        status = 'Online'
    except ConnectionError:
        status = 'Offline'

    finished = [job for job in JOB_LOG.values() if job.status == COMPLETE]

    return {
        'Redis status': status,
        'Number of jobs': len(JOB_LOG),
        'Number of finished jobs': finished
    }
