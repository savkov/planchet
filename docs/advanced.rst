👩‍🔬 Advanced
====================

Planchet generally makes things easier for you but sometimes it's really
difficult to configure it correctly. This page will discuss some advanced
topics that will help you understand how to do that correctly and hopefully
anticipate some of the common failures.

The ledger / Redis
^^^^^^^^^^^^^^^^^^

Planchet uses Redis as a ledger to log all jobs it manges as well as all the
items from these jobs.
The service uses a special instance of Redis that has persistence
pre-configured (see `here <https://quay.io/repository/savkov/redis>`_).
Planchet can also be run with a regular Redis instance without that feature,
of course.

The data stored in Redis takes the following shapes:

**Job**

.. code-block:: text

   key -> "JOB:<job_name>"
   value -> "{'metadata': '...','reader_name': '...','writer_name': '...','mode': '...'}"

**Item**

.. code-block:: text

   key -> "<job_name>: <item_id>"
   value -> 'SERVED' or 'RECEIVED' or 'ERROR'


Requests and batching
^^^^^^^^^^^^^^^^^^^^^

The service is running in a single process and all reading and writing is done
in a single thread. This presents some constraints on how the service can be
used.

**Batches:** the batches need to be set carefully as a batch size that is too
small would make the service block too easily if there is a large amount of
workers. A batch size that is too large could result in the service taking too
long to receive and write the bacth to disk, which would again block the
service. Ideally, one should set the batch size to a reasonable size that would
give the service enough processing time given the number of workers. This
sounds like a very dark art, but unless you are using a worker pool of many
tens or even hundreds of workers (or very large data items), you can just use
the default value of 100 and not worry about it.

**Rquests:** we alleviate the possible wrong "guessing" of the batch size by
simply retying the requests several times. This is built into
the :ref:`client <usage:The client>` and generally you don't need to worry
about it unless you feel you need to force a particular number of retries.

Data formats
^^^^^^^^^^^^

Currently, Planchet supports reading and writing only in two data formats:
JSONL and CSV. However, new readers and writers can be added in
the :ref:`io module <source/planchet:planchet.io>`
if they have the following signatures:

.. code-block:: python

   class MyReader:
       def __init__(self, meta_data: Dict):
           # The content of the metadata dictionary is not controlled.
           # You need to make sure that you pass the correct parameters to
           # your reader.

       def __call__(self, batch_size: int) -> List:
           # This method should return a list of items. The list should be
           # of length equal to the batch_size parameter.

   class MyWriter:
       def __init__(self, meta_data: Dict):
           # The content of the metadata dictionary is not controlled.
           # You need to make sure that you pass the correct parameters to
           # your writer.

       def __call__(self, data: List):
           # This method takes a list of items and writes them to disk.


As you can see, the reading/writing is not really constrained in any way.
In fact, you can easily implement your own classes that read and write from/to
a database, for example (probably won't work in docker unless you add the
appropriate dependencies though 🤭).

Data directories
^^^^^^^^^^^^^^^^

Planchet can access anything the user it run under can. This means that if you
run it on the bare metal and point it to a file, it will find it. If you are
using docker, however, you will need to mount a directory into the container
so the path to that file will change. By default docker will mount ``.data/``
in the Planchet directory to ``/data`` in the container. Make sure that you
get this right as the client will not complain in a very useful way.


Security
^^^^^^^^

As stated a few times in this documentation: Planchet is not a production tool.
The main reason for that is that it easily exposes the host to external jobs.
The considerations are different in the different ways of running Planchet.

**🐳 Docker:** when you run the service using docker, you are essentially giving
complete access to anything in the container to Planchet. Anything can be read
and re-written using a job. The good thing is that you probably won't have
anything useful in that container 🤷‍♂️.

**🐻🤘 Bare metal:** when you simply run Planchet using the ``make run`` route,
you will give Planchet (and its users) exactly the same access to the system
as the user you are doing it with. You may want to set up a special user
to protect your system (see
`this <https://askubuntu.com/questions/1082424/how-to-create-www-data-user>`_
for inspiration), but you should also remember to include possible data sources
and output destinations into its permissions.


Debugging
^^^^^^^^^

As a fairly young project, Planchet is not great at telling you want's wrong.
You will probably run into some trouble at some point, so instead of feeling
silly, go and read the logs. For docker you can use
``docker logs -f <planchet-container>`` to read the output of the system as
requests are coming in. If you are running it on the bare metal, well it's
probably where you're running it 🤷‍♂️.
