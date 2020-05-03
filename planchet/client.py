import json
from typing import Dict, List, Union, Tuple

from requests import Response

from .util import requests_retry_session


class PlanchetClient:
    """ The PlanchetClient object provides an easy connectivity to a Planchet
    instance. It is essentially a convenience wrapper around the
    `requests library <https://requests.readthedocs.io/en/master/>`_.

    :param url: Planchet URL, e.g. `<http://localhost:5005>`_

    Attributes:
        RETRIES:    Default number of retries for requests.
    """

    RETRIES = 5

    def __init__(self, url):
        self.url = url if url.endswith('/') else url + '/'

    def start_job(self, job_name: str, metadata: Dict, reader_name: str,
                  writer_name: str, clean_start: bool = False,
                  retries: int = 1, mode: str = 'read-write',
                  cont: bool = False) -> Response:
        """
        Starts a job.

        Some jobs that you could run are:

        **Regular job:** use `job_name`, `metadata`, `reader_name` &
        `writer_name`.

        **Repair job:** like regular but set `cont` to True.


        :param job_name: name of the job
        :param metadata: metadata for the reader and writer classes, typically
           including the input/output file paths and others.
        :param reader_name: name of the reader class, e.g. `CsvReader`.
        :param writer_name: name of the writer class, e.g. `CsvWriter`.
        :param clean_start: cleans all items in the ledger (Redis) before
           starting the job.
        :param retries: number of time to retry this request
        :param mode: io mode; possible values are `read`, `write`, and
           the default `read-write`
        :param cont: makes the job a repair job resetting the reader iterator
           and cleaning all served by not received items.
        :return: the server response
        """
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
        """
        Deletes all references to a job, including the job metadata and the
        items associated with it.

        :param job_name: job name
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        return session.get(url=f'{self.url}delete?job_name={job_name}')

    def clean_job(self, job_name: str, retries: int = RETRIES) -> Response:
        """
        Remove all items associated with a job

        :param job_name: job name
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        return session.get(url=f'{self.url}clean?job_name={job_name}')

    def get_job_report(self, job_name: str, retries: int = RETRIES
                       ) -> Response:
        """
        Request the job report from Planchet.

        Example report:

        .. code-block:: python

           {
              'served': 20,
              'received': 20,
              'status': 'IN_PROGRESS'
           }


        :param job_name: job name
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        response = session.get(url=f'{self.url}report?job_name={job_name}')
        if response.status_code == 200:
            return json.loads(response.text)

    def get(self, job_name: str, n_items: int, retries: int = RETRIES) -> List:
        """
        Request a batch of items from `job_name`.

        :param job_name: job name
        :param n_items: number of items in the batch
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        response = session.post(
            url=f'{self.url}serve?job_name={job_name}&batch_size={n_items}'
        )
        if response.status_code == 200:
            return json.loads(response.text)

    def send(self, job_name: str, items: List[Tuple[int, Union[Dict, List]]],
             overwrite: bool = False, retries: int = RETRIES) -> Response:
        """
        Send a batch of processed items from `job_name` to Planchet.

        :param job_name: job name
        :param items: processed items
        :param overwrite: overwrite the output file
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        return session.post(
            url=f'{self.url}receive?job_name={job_name}&overwrite={overwrite}',
            json=items
        )

    def mark_errors(self, job_name: str, ids: List[int],
                    retries: int = RETRIES):
        """
        Mark a list of item IDs as errors.

        :param job_name: job name
        :param ids: list of item IDs
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        return session.post(
            url=f'{self.url}mark-errors?job_name={job_name}',
            json=ids
        )

    def check(self, retries: int = RETRIES) -> Response:
        """
        Check if Planchet is healthy.

        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        response = session.get(url=f'{self.url}health')
        if response.status_code == 200:
            return json.loads(response.text)
