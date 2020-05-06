🏃‍♂️Quickstart
===============

This guide will take you through all the steps to set up a Planchet instance,
and run a simple `NER <https://en.wikipedia.org/wiki/Named-entity_recognition>`_
processing over sample text using a spaCy worker script.

On the server
^^^^^^^^^^^^^

On the server we need to install Planchet and download some news headlines data
in an accessible directory. Then we copy over the data 1000 times to make it
large.

.. code-block:: shell

   git clone https://github.com/savkov/planchet.git
   cd planchet
   mkdir data
   wget https://raw.githubusercontent.com/explosion/prodigy-recipes/master/example-datasets/news_headlines.jsonl -O data/news_headlines.jsonl
   python -c "news=open('data/news_headlines.jsonl').read();open('data/news_headlines.jsonl', 'w').write(''.join([news for _ in range(200)]))"
   export PLANCHET_REDIS_PWD=my-super-secret-password-%$^@
   make install
   make install-redis
   make run


Note that the service will run at `0.0.0.0:5005 <0.0.0.0:5005>`_ on your host
machine. If you want to use a different host or port, use the make parameters:

.. code-block:: shell

   make run HOST=my.host.com PORT=6000

**Note:** this guide will **not** work if you run a docker instance. If you do
want to do that, you will need to alter the script as indicated in the
comments below.

On the client
^^^^^^^^^^^^^

On the client side we need to install the Planchet client and
`spaCy <spacy.io>`_.

.. code-block:: shell

   pip install planchet spacy tqdm
   python -m spacy download en_core_web_sm
   export PLANCHET_REDIS_PWD=<your-redis-password>

Then we write the following script in a file called ``spacy_ner.py`` making sure
you fill in the placeholders.

.. code-block:: python

   from planchet import PlanchetClient
   import spacy
   from tqdm import tqdm

   nlp = spacy.load("en_core_web_sm")

   PLANCHET_HOST = '0.0.0.0'  # <--- CHANGE IF NEEDED
   PLANCHET_PORT = 5005

   url = f'http://{PLANCHET_HOST}:{PLANCHET_PORT}'
   client = PlanchetClient(url)

   job_name = 'spacy-ner-job'
   metadata = { # NOTE: this assumes planchet has access to this path
       'input_file_path': './data/news_headlines.jsonl',  # <--- change to /data/[...] if using docker
       'output_file_path': './data/entities.jsonl'  # <--- change to /data/[...] if using docker
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

Finally, we want to do some parallel processing with 8 processes. We can start
each process manually or we can use the `parallel` tool to start them all.

.. code-block:: shell

   seq -w 0 8 | parallel python spacy_ner.py {}
