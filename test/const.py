import os

SERVICE_NAME = 'service-name'
CSV_SIZE = 30
TEST_JOB_NAME = 'new_test_job'
TOKEN_TEST_JOB_NAME = 'token_new_test_job'
PLANCHET_HOST = os.environ.get('PLANCHET_HOST', 'localhost')
PLANCHET_PORT = os.environ.get('PLANCHET_PORT', 5005)
PLANCHET_TEST_DIR = os.environ.get('PLANCHET_TEST_DIR', '.')
