import json
import logging
from typing import Dict, List, Union, Tuple

from requests import Response

from .util import requests_retry_session


_fmt = '%(message)s'
logging.basicConfig(level=logging.DEBUG, format=_fmt)


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
                  token: Union[str, None] = None,
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
        :param token: authentication token; no authentication if empty
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
        if token is not None:
            params['token'] = token
        url = self.make_param_url('scramble', params)
        session = requests_retry_session(retries=retries)
        return session.post(url=url, json=metadata)

    def delete_job(self, job_name: str, token: Union[str, None] = None,
                   retries: int = RETRIES) -> Response:
        """
        Deletes all references to a job, including the job metadata and the
        items associated with it.

        :param job_name: job name
        :param token: authentication token; no authentication if empty
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        params = {'job_name': job_name}
        if token is not None:
            params['token'] = token
        url = self.make_param_url('delete', params)
        return session.get(url=url)

    def clean_job(self, job_name: str, token: Union[str, None] = None,
                  retries: int = RETRIES) -> Response:
        """
        Remove all items associated with a job

        :param job_name: job name
        :param token: authentication token; no authentication if empty
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        params = {'job_name': job_name}
        if token is not None:
            params['token'] = token
        url = self.make_param_url('clean', params)
        return session.get(url=url)

    def purge_server(self, master_token: str, output: bool = True,
                     retries: int = RETRIES) -> Response:
        """
        Remove all jobs, items and optionally delete all output files from
        the server.

        :param master_token: master authentication token for the server
        :param output: deletes output file if true
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        params = {'token': master_token, 'output': output}
        url = self.make_param_url('purge', params)
        return session.get(url=url)

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

    def get(self, job_name: str, n_items: int,
            token: Union[str, None] = None,
            retries: int = RETRIES) -> List:
        """
        Request a batch of items from `job_name`.

        :param job_name: job name
        :param n_items: number of items in the batch
        :param token: authentication token; no authentication if empty
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        params = {'job_name': job_name, 'batch_size': n_items}
        if token is not None:
            params['token'] = token
        url = self.make_param_url('serve', params)
        response = session.post(url=url)
        if response.status_code == 200:
            return json.loads(response.text)

    def send(self, job_name: str, items: List[Tuple[int, Union[Dict, List]]],
             token: Union[str, None] = None,
             overwrite: bool = False, retries: int = RETRIES) -> Response:
        """
        Send a batch of processed items from `job_name` to Planchet.

        :param job_name: job name
        :param items: processed items
        :param token: authentication token; no authentication if empty
        :param overwrite: overwrite the output file
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        if overwrite:
            logging.warning('The overwrite parameter is discouraged and will '
                            'be removed in the next major release.')
        params = {'job_name': job_name, 'overwrite': overwrite}
        if token is not None:
            params['token'] = token
        url = self.make_param_url('receive', params)
        return session.post(url=url, json=items)

    def mark_errors(self, job_name: str, ids: List[int],
                    token: Union[str, None] = None,
                    retries: int = RETRIES):
        """
        Mark a list of item IDs as errors.

        :param job_name: job name
        :param ids: list of item IDs
        :param token: authentication token; no authentication if empty
        :param retries: number of retries for this request
        :return: the server response
        """
        session = requests_retry_session(retries=retries)
        params = {'job_name': job_name}
        if token is not None:
            params['token'] = token
        url = self.make_param_url('mark-errors', params)
        return session.post(url=url, json=ids)

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

    def make_param_url(self, endpoint, params):
        params_str = '&'.join(f'{k}={v}' for k, v in params.items())
        return f'{self.url}{endpoint}?{params_str}'
