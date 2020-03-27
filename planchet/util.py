from typing import Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: Tuple = (500, 502, 504),
    session: Union[requests.Session, None] = None,
) -> requests.Session:
    session: requests.Session = session or requests.Session()
    retry: Retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter: HTTPAdapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# COLORS

def red(s):
    return '\033[91m' + str(s) + '\033[0m'


def yellow(s):
    return '\033[93m' + str(s) + '\033[0m'


def green(s):
    return '\033[92m' + str(s) + '\033[0m'


def blue(s):
    return '\033[94m' + str(s) + '\033[0m'


def pink(s):
    return '\033[95m' + str(s) + '\033[0m'


def lightblue(s):
    return '\033[96m' + str(s) + '\033[0m'


def white(s):
    return '\033[97m' + str(s) + '\033[0m'


def underline(s):
    return '\033[4m' + str(s) + '\033[0m'


def bold(s):
    return '\033[1m' + str(s) + '\033[0m'


def light(s):
    return '\033[2m' + str(s) + '\033[0m'


def flash(s):
    return '\033[5m' + str(s) + '\033[0m'


def orangefill(s):
    return '\033[100m' + str(s) + '\033[0m'


def redfill(s):
    return '\033[101m' + str(s) + '\033[0m'


def greenfill(s):
    return '\033[102m' + str(s) + '\033[0m'


def yellowfill(s):
    return '\033[103m' + str(s) + '\033[0m'


def bluefill(s):
    return '\033[104m' + str(s) + '\033[0m'


def pinkfill(s):
    return '\033[105m' + str(s) + '\033[0m'


def lightbluefill(s):
    return '\033[106m' + str(s) + '\033[0m'


def whitefill(s):
    return '\033[107m' + str(s) + '\033[0m'
