import json
import logging
from typing import Callable, List, Dict, Union, Tuple

from redis import Redis

from planchet import io

_fmt = '%(message)s'
logging.basicConfig(level=logging.DEBUG, format=_fmt)

SERVED = 'SERVED'
RECEIVED = 'RECEIVED'
ERROR = 'ERROR'

IN_PROGRESS = 'IN_PROGRESS'
COMPLETE = 'COMPLETE'

READ_ONLY = 'read'
WRITE_ONLY = 'write'
READ_WRITE = 'read-write'


class Job:
    """
    Create a new Job object.

    :param name: job name
    :param reader: reader object
    :param writer: writer object
    :param ledger: ledger object
    :param mode: writing mode
    :param cont: make a repair job if True
    """
    def __init__(self, name: str, reader: Callable, writer: Callable,
                 ledger: Redis, mode: str = READ_WRITE,
                 cont: bool = False):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.ledger = ledger
        self.mode = mode
        self.served = set()
        self.received = set()
        self.exhausted = False
        self.cont = cont
        self.restore_records(self)

    def serve(self, n_items: int) -> List:
        """
        Send `n_items` to the user.

        :param n_items: number of items served
        :return: list of items of requested size
        """
        items: List = []
        while len(items) < n_items:
            bs = n_items - len(items)
            buff = self.reader(bs)
            for id_, item in buff:
                status = self.ledger.get(self.ledger_id(id_))
                if not bool(status) or (
                        self.cont and
                        status and
                        status.decode('utf8') == SERVED
                ):
                    items.append((id_, item))
                    self.ledger.set(self.ledger_id(id_), SERVED)
                    self.served.add(id_)
            if not buff:
                self.exhausted = True
                break
        return items

    def receive(self, items: List[Tuple[int, Union[Dict, List]]],
                overwrite: bool):
        """
        Receive a list of processed items from a job, write them to output and
        mark them as received.

        :param items: processed items
        :param overwrite: overwrite the output file
        """
        ids = []
        data = []
        for id_, item in items:
            # This will skip writing data for records that have been written
            # already based on the id's in the ledger. This does not apply to
            # dumping jobs.
            if self.mode == READ_WRITE and not overwrite and \
                    self.ledger.get(
                        self.ledger_id(id_)).decode('utf8') == RECEIVED:
                continue
            ids.append(id_)
            data.append(item)
        self.writer(data)
        for id_ in ids:
            self.received.add(id_)
            self.served.discard(id_)
            self.ledger.set(self.ledger_id(id_), RECEIVED)

    def mark_errors(self, ids):
        """
        Mark the items with IDs in `ids` as errors.

        :param ids: IDs of items to be marked as errors
        """
        for id_ in ids:
            value = self.ledger.get(self.ledger_id(id_))
            if value and value.decode('utf8') == RECEIVED:
                logging.error(f'Attempting to mark a received item: {id_}')
                raise ValueError(f'Item already received: {id_}')
            self.ledger.set(self.ledger_id(id_), ERROR)

    def restart(self):
        """
        Restart the job. The ledger is wiped, all items in this object are
        cleaned and the job is set to not exhausted.
        """
        for key in list(self.ledger.scan_iter(f'{self.name}:*')):
            self.ledger.delete(key)
        self.served = set()
        self.received = set()
        self.exhausted = False

    def clean(self, output: bool = True):
        """
        Cleans the job. All served but not received items are cleaned from the
        ledger.

        :param output: remove the output file for this job
        """
        served = []
        q_string = f'{self.name}:*'
        for i, key in enumerate(self.ledger.scan_iter(q_string)):
            value = self.ledger.get(key).decode('utf8')
            if value == SERVED:
                served.append(key)
        for key in served:
            self.ledger.delete(key)
            _, id_ = key.decode('utf8').split(':', 1)
            self.served.discard(int(id_))
        if output:
            self.writer.clean()

    @property
    def status(self):
        """
        Returns the status of this job: ``COMPLETE`` or ``IN_PROGRESS``.

        :return: job status
        """
        if self.exhausted and not self.served:
            return COMPLETE
        else:
            return IN_PROGRESS

    @property
    def stats(self):
        """
        Returns the report of this job: # items served and received, as well as
        if the job is done.

        :return: job report
        """
        return {
            'served': len(self.served),
            'received': len(self.received),
            'status': self.status
        }

    def ledger_id(self, id_: Union[str, int]) -> str:
        return f'{self.name}:{id_}'

    @staticmethod
    def restore_records(job):
        keys = job.ledger.scan_iter(f'{job.name}:*')
        for key in keys:
            id_ = int(key.decode('utf8').split(':', 1)[1])
            value = job.ledger.get(key).decode('utf8')
            if value == SERVED:
                job.served.add(id_)
            elif value == RECEIVED:
                job.received.add(id_)

    @staticmethod
    def restore_job(job_name: str, job_key: str, ledger: Redis):
        record = ledger.get(job_key)
        if not record:
            return
        record = json.loads(record.decode('utf8'))
        reader_name = record['reader_name']
        writer_name = record['writer_name']
        metadata = record['metadata']
        reader: Callable = get_io_object(reader_name, metadata)
        writer: Callable = get_io_object(writer_name, metadata)
        mode: str = record['mode']
        job: Job = Job(job_name, reader, writer, ledger, mode)
        return job


def get_io_object(name, metadata):
    try:
        return getattr(io, name)(metadata)
    except AttributeError:
        return None
