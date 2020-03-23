import os


REDIS_HOST = os.environ.get('PLANCHET_REDIS_HOST', 'localhost')
REDIS_PORT = os.environ.get('PLANCHET_REDIS_PORT', '6379')
REDIS_PWD = os.environ.get('PLANCHET_REDIS_PWD')

MAX_PACKAGE_SIZE = int(os.environ.get('PLANCHET_MAX_PACKAGE_SIZE', 10)) * 10**6
