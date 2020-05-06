import json
import logging
import sys
from typing import List, Callable, Dict, Tuple, Union

from fastapi import FastAPI, HTTPException
from redis import Redis
from redis.exceptions import ConnectionError

from planchet.core import Job, COMPLETE, READ_ONLY, WRITE_ONLY, READ_WRITE
from planchet.config import REDIS_HOST, REDIS_PORT, REDIS_PWD, MAX_PACKAGE_SIZE
import planchet.io as io
import planchet.util as util

_fmt = '%(message)s'
logging.basicConfig(level=logging.DEBUG, format=_fmt)

app = FastAPI()

logging.info(util.blue('PLANCHET IS STARTING!'))


def _load_jobs(ledger) -> Dict:
    jobs: Dict = {}
    for job_key in ledger.scan_iter(f'JOB:*'):
        job_name: str = job_key.decode('utf8').split(':', 1)[1]
        try:
            jobs[job_name] = Job.restore_job(job_name, job_key, ledger)
        except json.JSONDecodeError:
            logging.error(f'Could not restore job: {job_key}')
        except FileNotFoundError as e:
            logging.error(f'Could not restore job: {job_key}; {e}')
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
    logging.critical(
        util.redfill(f'Could not connect to redis at {REDIS_HOST}:{REDIS_PORT}'
                     f' using a password that was'
                     f'{" " if REDIS_PWD is None else " not"} None.'))


@app.post("/scramble")
def scramble(job_name: str, metadata: Dict, reader_name: str,
             writer_name: str, clean_start: bool = False,
             mode: str = READ_WRITE, cont: bool = False):
    """
    Start a new job.

    :param job_name: job name
    :param metadata: I/O classes configuration
    :param reader_name: class reader name
    :param writer_name: class writer name
    :param clean_start: clean the items before you start
    :param mode: I/O mode
    :param cont: start a repair job
    """
    logging.info(util.pink(
        f'SCRAMBLING: name->{job_name}; metadata->{metadata}; '
        f'reader_name->{reader_name}; writer_name->{writer_name}; '
        f'clean_start->{clean_start}'))
    try:
        reader: Callable = getattr(io, reader_name)(metadata) \
            if reader_name else None
        writer: Callable = getattr(io, writer_name)(metadata) \
            if writer_name else None
    except FileNotFoundError as e:
        logging.error(e)
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        logging.error(e)
        raise HTTPException(status_code=400, detail=str(e))
    existing_job = LEDGER.get(f'JOB:{job_name}')
    # trying to re-create an existing job
    if existing_job and (not clean_start and not cont):
        msg = f'Job {job_name} already exists.'
        raise HTTPException(status_code=400, detail=msg)
    # continue where you left off
    if existing_job and cont:
        job = JOB_LOG[job_name]
        job.clean()
        del job
        del JOB_LOG[job_name]
    new_job: Job = Job(job_name, reader, writer, LEDGER, mode, cont)
    # clean ledger before starting
    if clean_start:
        new_job.restart()
    JOB_LOG[job_name] = new_job
    # TODO: move this in a method inside the Job class and call here
    LEDGER.set(f'JOB:{job_name}', json.dumps({
        'metadata': metadata, 'reader_name': reader_name,
        'writer_name': writer_name, 'mode': mode}))


@app.post("/serve")
def serve(job_name: str, batch_size: int = 100) -> List:
    """
    Serve a batch of items to the user.

    :param job_name: job name
    :param batch_size: number of items to be served in the batch
    :return: list of items of size `batch_size`
    """
    try:
        job = JOB_LOG[job_name]
    except KeyError:
        active = LEDGER.get(job_name)
        no_active_msg = f'No active job: {job_name}'
        no_known_msg = f'No known job: {job_name}'
        msg = no_active_msg if active else no_known_msg
        raise HTTPException(status_code=400, detail=msg)
    if job.mode == WRITE_ONLY:
        raise HTTPException(400, 'Trying to read from a write-only job')
    return job.serve(batch_size)


@app.post("/receive")
def receive(job_name: str, items: List[Tuple[int, Union[Dict, List]]],
            overwrite: bool):
    """
    Receive a batch of processed items from the user.

    :param job_name: job name
    :param items: processed items
    :param overwrite: overwrite the output file
    """
    size = sys.getsizeof(items)
    if size > MAX_PACKAGE_SIZE:
        msg = f'In-memory payload must be less than {MAX_PACKAGE_SIZE}; ' \
              f'the current size is {size}.'
        logging.error(util.red(msg))
        raise HTTPException(status_code=413, detail=msg)
    job = JOB_LOG[job_name]
    if job.mode == READ_ONLY:
        raise HTTPException(400, 'Trying to send to a read-only job')
    if not job.writer:
        raise HTTPException(400, 'No valid writer initialised')
    job.receive(items, overwrite)
    if job.status == COMPLETE:
        LEDGER.set(_job_id(job_name), COMPLETE)


@app.post("/mark-errors")
def mark_errors(job_name: str, ids: List[int]):
    """
    Mark a list of items as errors based on the IDs in `ids`.

    :param job_name: job name
    :param ids: list of IDs
    """
    job = JOB_LOG[job_name]
    try:
        job.mark_errors(ids)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get('/delete')
def delete(job_name: str):
    """
    Delete a job.

    :param job_name: job name
    """
    try:
        del JOB_LOG[job_name]
    except KeyError:
        logging.info(util.pink(f'Could not find a job named "{job_name}"'))
        pass
    LEDGER.delete(f'JOB:{job_name}')
    for record in list(LEDGER.scan_iter(f'{job_name}:*')):
        LEDGER.delete(record)


@app.get('/clean')
def clean(job_name: str):
    """
    Remove all served but not received items from a job.

    :param job_name: job name
    """
    try:
        job = JOB_LOG[job_name]
    except KeyError:
        msg = f'Could not find a job name "{job_name}"'
        logging.info(util.pink(msg))
        raise HTTPException(400, msg)
    job.clean()


@app.get("/report")
def report(job_name: str) -> Dict:
    """
    Serve a report for a job

    :param job_name: job name
    :return: report
    """
    try:
        return JOB_LOG[job_name].stats
    except KeyError:
        logging.info(util.pink(f'Could not find a job named "{job_name}"'))
        return {}


@app.get("/health")
def health_check() -> Dict:
    """
    Service health check. Healthy if a live ledger can be reached.

    :return: service status
    """
    try:
        LEDGER.ping()
        status = 'Online'
    except AttributeError:
        logging.critical(util.redfill('REDIS IS OFFLINE'))
        status = 'Offline'

    finished = [job for job in JOB_LOG.values() if job.status == COMPLETE]

    return {
        'Redis status': status,
        'Number of jobs': len(JOB_LOG),
        'Number of finished jobs': finished
    }
