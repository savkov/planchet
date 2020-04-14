import json
import logging
import os
import threading
from typing import Dict, List, Iterator, Union

import pandas as pd
from pandas.io.parsers import TextFileReader

from .util import red


class CsvReader:
    def __init__(self, meta_data: Dict):
        self.file_path: str = meta_data['input_file_path']
        self.chunk_size: int = int(meta_data.get('chunk_size', 100))
        self.file_iter: TextFileReader = \
            pd.read_csv(self.file_path, iterator=True,
                        chunksize=self.chunk_size)
        self.df_iter = self._next_fp_it()
        self.idx = 0
        self.lock = threading.Lock()

    def _next_fp_it(self):
        return next(self.file_iter).iterrows()

    def _iterator(self):
        while True:
            for _, row in self.df_iter:
                idx = self.idx
                self.idx += 1
                yield idx, row
            try:
                self.df_iter = self._next_fp_it()
            except StopIteration:
                break

    def __call__(self, batch_size: int):
        with self.lock:
            batch: List = []
            for id_, row in self._iterator():
                batch.append((id_, (*row,)))
                if len(batch) == batch_size:
                    break
            return batch


class JsonlReader:
    def __init__(self, meta_data: Dict):
        self.id_ = 0
        self.iter: Iterator = open(meta_data['input_file_path'])
        self.lock = threading.Lock()

    def __call__(self, batch_size: int):
        with self.lock:
            batch: List = []
            for id_, line in enumerate(self.iter, start=self.id_):
                try:
                    jsn: Union[Dict, List] = json.loads(line)
                except json.JSONDecodeError:
                    logging.error(red(f'Could not parse JSON: {line}'))
                    continue
                batch.append((id_, jsn))
                self.id_ = id_ + 1
                if len(batch) == batch_size:
                    break
            return batch


class CsvWriter:
    def __init__(self, metadata: Dict):
        self.file_path: str = metadata['output_file_path']
        overwrite: bool = metadata.get('overwrite', False)
        exists = os.path.exists(self.file_path)
        self.mode = 'w' if overwrite else 'a'
        self.has_header = overwrite or not exists

    def __call__(self, data: List):
        if not data:
            return
        header = data[0] if self.has_header else None
        records = data[1:] if self.has_header else data
        df = pd.DataFrame(records, columns=header)
        df.to_csv(self.file_path, mode=self.mode, header=self.has_header,
                  index=False)


class JsonlWriter:
    def __init__(self, metadata: Dict):
        self.file_path: str = metadata['output_file_path']
        overwrite: bool = metadata.get('overwrite', False)
        self.mode = 'w' if overwrite else 'a'

    def __call__(self, data: List):
        with open(self.file_path, mode=self.mode) as fh:
            fh.write('\n'.join([json.dumps(jsn) for jsn in data]))
            fh.write('\n')
