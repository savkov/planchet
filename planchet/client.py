import json
from typing import Dict, List, Union, Tuple

from requests import Response

from .util import requests_retry_session


class PlanchetClient:

    RETRIES = 5

    def __init__(self, url):
        self.url = url if url.endswith('/') else url + '/'

    def start_job(self, job_name: str, metadata: Dict, reader_name: str,
                  writer_name: str, clean_start: bool = False,
                  retries: int = 1, mode: str = 'read-write',
                  cont: bool = False) -> Response:
        params = {
            'job_name': job_name,
            'reader_name': reader_name,
            'writer_name': writer_name,
            'mode': mode,
            'clean_start': clean_start,
            'cont': cont
        }
        param_string = '&'.join([f'{k}={v}' for k, v in params.items()])
        session = requests_retry_session(retries=retries)
        return session.post(
            url=f'{self.url}scramble?{param_string}', json=metadata
        )

    def delete_job(self, job_name: str, retries: int = RETRIES) -> Response:
        session = requests_retry_session(retries=retries)
        return session.get(url=f'{self.url}delete?job_name={job_name}')

    def clean_job(self, job_name: str, retries: int = RETRIES) -> Response:
        session = requests_retry_session(retries=retries)
        return session.get(url=f'{self.url}clean?job_name={job_name}')

    def get_job_report(self, job_name: str, retries: int = RETRIES
                       ) -> Response:
        session = requests_retry_session(retries=retries)
        response = session.get(url=f'{self.url}report?job_name={job_name}')
        if response.status_code == 200:
            return json.loads(response.text)

    def get(self, job_name: str, n_items: int, retries: int = RETRIES) -> List:
        session = requests_retry_session(retries=retries)
        response = session.post(
            url=f'{self.url}serve?job_name={job_name}&batch_size={n_items}'
        )
        if response.status_code == 200:
            return json.loads(response.text)

    def send(self, job_name: str, items: List[Tuple[int, Union[Dict, List]]],
             overwrite: bool = False, retries: int = RETRIES) -> Response:
        session = requests_retry_session(retries=retries)
        return session.post(
            url=f'{self.url}receive?job_name={job_name}&overwrite={overwrite}',
            json=items
        )

    def mark_errors(self, job_name: str, ids: List[int],
                    retries: int = RETRIES):
        session = requests_retry_session(retries=retries)
        return session.post(
            url=f'{self.url}mark-errors?job_name={job_name}',
            json=ids
        )

    def check(self, retries: int = RETRIES) -> Response:
        session = requests_retry_session(retries=retries)
        response = session.get(url=f'{self.url}health')
        if response.status_code == 200:
            return json.loads(response.text)
