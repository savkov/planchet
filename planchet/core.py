import json
from typing import Callable, List, Dict, Union, Tuple

from redis import Redis

from planchet import io as io_module

SERVED = 'SERVED'
RECEIVED = 'RECEIVED'

IN_PROGRESS = 'IN_PROGRESS'
COMPLETE = 'COMPLETE'

READ_ONLY = 'read'
WRITE_ONLY = 'write'
READ_WRITE = 'read-write'


class Job:
    def __init__(self, name: str, reader: Callable, writer: Callable,
                 ledger: Redis, io: str = READ_WRITE):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.ledger = ledger
        self.io = io
        self.served = set()
        self.received = set()
        self.exhausted = False
        self.restore_records(self)

    def serve(self, n_items: int) -> List:
        items: List = []
        while len(items) < n_items:
            buff = self.reader(n_items - len(items))
            for id_, item in buff:
                self.served.add(id_)
                if not self.ledger.exists(self.ledger_id(id_)):
                    items.append((id_, item))
                    self.ledger.set(self.ledger_id(id_), SERVED)
            if not buff:
                self.exhausted = True
                break
        return items

    def receive(self, items: List[Tuple[int, Union[Dict, List]]],
                overwrite: bool):
        ids = []
        data = []
        for id_, item in items:
            # This will skip writing data for records that have been written
            # already based on the id's in the ledger. This does not apply to
            # dumping jobs.
            if self.io == READ_WRITE and not overwrite and \
                    self.ledger.get(
                        self.ledger_id(id_)).decode('utf8') == RECEIVED:
                continue
            ids.append(id_)
            data.append(item)
        self.writer(data)
        for id_ in ids:
            self.received.add(id_)
            self.ledger.set(self.ledger_id(id_), RECEIVED)

    def restart(self):
        for key in self.ledger.scan_iter(f'{self.name}:*'):
            self.ledger.delete(key)
        self.served = set()
        self.received = set()
        self.exhausted = False

    @property
    def status(self):
        if self.exhausted and len(self.served) == len(self.received):
            return COMPLETE
        else:
            return IN_PROGRESS

    @property
    def stats(self):
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
            _, id_ = key.decode('utf8').split(':', 1)
            value = job.ledger.get(key).decode('utf8')
            if value == SERVED:
                job.served.add(id_)
            elif value == RECEIVED:
                job.served.add(id_)
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
        reader: Callable = getattr(io_module, reader_name)(metadata)
        writer: Callable = getattr(io_module, writer_name)(metadata)
        dumping: bool = record['dumping']
        job: Job = Job(job_name, reader, writer, ledger, dumping)
        return job
