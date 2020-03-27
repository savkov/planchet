# Planchet
_Your large data processing personal assistant_

[![CircleCI](https://circleci.com/gh/savkov/planchet.svg?style=svg)](https://circleci.com/gh/savkov/planchet)
[![Maintainability](https://api.codeclimate.com/v1/badges/4291c3334f1699a4f227/maintainability)](https://codeclimate.com/github/savkov/planchet/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/4291c3334f1699a4f227/test_coverage)](https://codeclimate.com/github/savkov/planchet/test_coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## About

Planchet (pronounced /plʌ̃ʃɛ/) is a data package manager suited for processing large arrays of data
items. It supports natively reading and writing into CSV and JSONL data files
and serving their content over a FastAPI service to clients that process the
data. It is a tool for scientist and hackers, not production. 

## How it works

Planchet solves the controlled processing of large amounts of data in a simple
and slightly naive way by controlling the reading and writing of the data as
opposed to the processing. When you create a job with Planchet you tell the 
sevice where to read, where to write and what classes to use for that. Next,
you (using the client or simple HTTP requests) ask the service for _n_ data 
items, which your process works through locally. When your processing is done,
it ships the items back to Planchet, who writes them to disk. All jobs and 
serving and receiving of items is logged in a Redis instance with persistence.
This assures that if you stop processing you will only lose the processing of
the data that was not sent back to Planchet. Planchet will automatically resume
jobs and skip over processed items.

_Caveat:_ Planchet is running in a single thread to avoid the mess of multiple
processes writing in the same file. Until this is fixed (may be never) you
should be careful with your batch sizes -- keep them not too big and not too 
small.

![diagram](https://github.com/savkov/planchet/blob/master/img/Planchet.png)

## Installation

Planchet works in two components: a service and a client. The service is the
core that does all the work managing the data while the client is a light
wrapper around `requests` that makes it easy to access the service API.

### Service

```bash
git clone git@github.com:savkov/planchet.git
export REDIS_PWD=<some-password>
make install
make run-redis
make run
```

If you want to run Planchet on a different port, you can use the `uvicorn` 
command but note that you **MUST** use only one worker. 

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 5005 --workers 1
```

### Client

```bash
pip install planchet
```

## Example


### On the server

On the server we need to install Planchet and download some news headlines data
in an accessible directory. Then we multiply the data 1000 times as there are 
only 200 lines originally. Don't forget to set your _Redis password_ before
you do `make install-redis`! 
```bash
git clone https://github.com/savkov/planchet.git
cd planchet
mkdir data
wget https://raw.githubusercontent.com/explosion/prodigy-recipes/master/example-datasets/news_headlines.jsonl -O data/news_headlines.jsonl
python -c "news=open('data/news_headlines.jsonl').read();open('data/news_headlines.jsonl', 'w').write(''.join([news for _ in range(1000)]))"
export PLANCHET_REDIS_PWD=<your-redis-password>
make install
make install-redis
make run
```

Note that planchet will run at port 5005 on your host machine.

### On the client

On the client side we need to install the Planchet client and [spaCy](spacy.io).

```bash
pip install planchet spacy tqdm
python -m spacy download en_core_web_sm
export PLANCHET_REDIS_PWD=<your-redis-password>

```
Then we write the following script in a file called `spacy_ner.py` making sure 
you fill in the placeholders.

```python
from planchet import PlanchetClient
import spacy
from tqdm import tqdm

nlp = spacy.load("en_core_web_sm")

PLANCHET_HOST = '<your host address>'  # fill in the host
PLANCHET_PORT = 5005

url = f'http://{PLANCHET_HOST}:{PLANCHET_PORT}'
client = PlanchetClient(url)

job_name = 'spacy-ner-job-3'
metadata = {  # fill in metadata
    'input_file_path': '<path-to-planchet-repo>/data/news_headlines.jsonl',
    'output_file_path': '<path-to-planchet-repo>/data/entities.jsonl'
}

# make sure you don't use the clean_start option here
client.start_job(job_name, metadata, 'JsonlReader', writer_name='JsonlWriter')

# make sure the number of items is large enough to avoid blocking the server
n_items = 100
headlines = client.get(job_name, n_items)

while headlines:
    ents = []
    print('Processing headlines batch...')
    for id_, item in tqdm(headlines):
        item['ents'] = [ent.text for ent in nlp(item['text']).ents]
        ents.append((id_, item))
    client.send(job_name, ents)
    headlines = client.get(job_name, n_items)

```

Finally, we want to do some parallel processing with 8 processes. We can start
each process manually or we can use the `parallel` tool to start them all.

```bash
seq -w 0 8 | parallel python spacy_ner.py {}
```
