🖥️ Installation
===============

Planchet works in two components: a *service* and a *client*.
The service is the core that does all the work managing the data,
while the client is a light wrapper around the
`requests <https://requests.readthedocs.io/en/master/>`_ library
that makes accessing the service safer and more convenient.

Service Side
^^^^^^^^^^^^

You can use this repo and start straight away like this:

.. code-block:: shell

   git clone git@github.com:savkov/planchet.git
   export PLANCHET_REDIS_PWD=my-super-secret-password-%$^@
   make install
   make run-redis
   make run

If you want to run Planchet on a different port, you can use the `uvicorn`
command but note that you **MUST** use only one worker.

.. code-block:: shell

   uvicorn app:app --reload --host 0.0.0.0 --port 5005 --workers 1


You can also run docker-compose from the git repo:

.. code-block:: shell

   git clone git@github.com:savkov/planchet.git
   export PLANCHET_REDIS_PWD=my-super-secret-password-%$^@
   docker-compose up

Client Side
^^^^^^^^^^^

Install the client from PyPi using:

.. code-block:: shell

   pip install planchet
