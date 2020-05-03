🧰 Usage
============

Here we discuss all necessary elements of setting up a Planchet processing job.
If you are looking for a complete example, look at `Quickstart <quickstart>`_.
If you are in a hurry, skip to the :ref:`Client section <usage:The client>`.
If you want a quick overview of what is possible, you can take a look at the
:ref:`PlanchetClient <source/planchet:planchet.client>` object or at the
Swagger API page typically under `<http://localhost:5005/docs>`_.

Jobs
^^^^

The job is essentially a big of metadata that controls the data management on
the server side. It is created though a separate request before starting to
do processing. To set up a regular job you will need a name, and to specify
the reading and writing methods through the parameters. There are, however,
some other types of jobs you may want to run, like an error logging job or
a reading job, or a repair job. Here's a list of all parameters relevant for
setting up a job:

- `name`: the name of the job. If the job already exists, your initial request will fail.
- `reader_name`: the name of the reader class, e.g. ``CsvReader``.
- `writer_name`: the name of the writer class, e.g. ``CsvWriter``.
- `metadata`: the metadata passed on the reader and the writer classes (see more :ref:`below <usage:Readers & writers>`).
- `clean_start`: restarts the job if it exists.
- `mode`: I/O mode for the job. Possible values: ``read``, ``write``, ``read-write``.
- `cont`: if true, resets the iterator of the reader and allows to serve all incomplete items again.

Now that we know what the parameters of a job are, let's consider the scenarios
we mentioned above.

**Reading job:** if you are interested in reading data from Planchet and not
storing the results, you can set the ``mode`` parameter to ``read`` which will
remove the requirement of specifying a writer.

**Error logging job:** if you just want to dump things through Planchet, like
your errors, you can set the ``mode`` parameter to ``write`` and disable the
reader.

**Repair job:** you will likely have a case where you interrupt a process or
you crash the system but you want to continue your processing. Typically,
Planchet will just give you the next items available and simply ignore the ones
that it served but never received. Making this smart has a lot of complications
so instead we handle it by running a repair job at the end using the ``cont``
parameter. It essentially resets the iterator and wipes all non-complete items
from the log. Then it goes and reads them again while skipping all the complete
items.

The data
^^^^^^^^

You can currently process anything you want that can be read and written to
a CSV or a JSONL file. The :ref:`constraints <intro:Constraints>` to this are
basically independence of the data points and considerate sizes of the items
and the workers pool.

You could process data from/to other types of format if you build your own
reader and writer as discussed in
the :ref:`advanced section <advanced:Data formats>`.

In practical terms, the data is served in two formats based on whether it comes
from a CSV or JSONL file:

.. code-block:: python

   items = client.get(job_name, n_items)
   for id_, item in items:
       print(item)
       # prints a list if reading from a CSV file
       # prints a dictionary or a list if reading from a JSONL file



Readers & writers
^^^^^^^^^^^^^^^^^

Planchet currently supports CSV through the ``CsvReader`` and ``CsvWriter``
classes, and JSONL through the ``JsonlReader`` and ``JsonlWriter`` classes.
You need to specify one of each pair as the name of your reader and writer
in order to confuigure a job. You will also need to provide a shared metadata
file for the reader and the writer, which is essentially a configuration.

Currently, the following parameters are used:

- `input_file_path`: path to the data file for the job (both formats)
- `output_file_path`: path to the output file for the job (both formats)
- `chunk_size`: size of the chunk to be read by the CSV reading iterator; you probably don't need to worry about this one.
- `overwrite`: if true, existing files are overwritten; if false existing files are appended.

**Example**

.. code-block:: python

   {
     "input_file_path": "/path/to/file",
     "output_file_path": "/path/to/output",
     "chunk_size": 100,
     "overwrite": False
   }

The endpoints
^^^^^^^^^^^^^

**scramble:** starts a job. Requires ``name``, ``reader_name``,
``writer_name``, and ``metadata`` parameters. Can be further parametrised by
``cont`` to make a repair job and ``mode`` to control whether it will be a
read-only, write-only or read and write job.

**/serve:** serves a batch of items from a job (``job_name``). The number of
items depends on the ``batch_size``.

**/receive:** receives a batch of items from a job (``job_name``) sent through
the ``items`` parameter.

**/mark_errors:** marks items from job ``job_name`` spacified in ``ids`` as
errors.

**/delete:** deletes ``job_name`` and all items associated with it. Does not
clean the output file.

**/clean:** deletes all items assiciated with ``job_name``.

**/report:** returns the status of ``job_name`` and numbers of completed items
and currently in flight.

**/health_check:** checks if the service is healthy.

The client
^^^^^^^^^^

It is possible to use Planchet by directly querying the API endpoints, but it
is much more convenient to use
the :ref:`PlanchetClient <source/planchet:planchet.client>` object.
This section will briefly show how to create and execute a regular job and how
a to change it to a repair job using the client.
For a full description of all methods, refer to the
:ref:`source documentation <source/planchet:planchet.client>`.

**Example**

.. code-block:: python

   from planchet import PlanchetClient
   from tqdm import tqdm

   PLANCHET_HOST = '0.0.0.0'  # <--- CHANGE IF NEEDED
   PLANCHET_PORT = 5005  #  <-- CHANGE IF NEEDED

   url = f'http://{PLANCHET_HOST}:{PLANCHET_PORT}'
   client = PlanchetClient(url)

   job_name = 'regular-job'
   metadata = {
       'input_file_path': '/data/data.jsonl',
       'output_file_path': '/data/output.jsonl'
   }

   # make sure you don't use the clean_start option here
   # to make this a REPAIR JOB, set --> cont=True
   client.start_job(job_name, metadata, 'JsonlReader', writer_name='JsonlWriter')

   # make sure the number of items is large enough to avoid blocking the server
   n_items = 100
   items = client.get(job_name, n_items)

   while items:
       processed = []
       print('Processing item batch...')
       for id_, item in tqdm(items):
           item['hash'] = hash(item['text'])
           processed.append((id_, item))
       client.send(job_name, processed)
       items = client.get(job_name, n_items)

