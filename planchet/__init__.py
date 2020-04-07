from .client import PlanchetClient  # noqa

# It's a trade-off, ugly code vs. convenience.
try:
    from .core import Job  # noqa
except ModuleNotFoundError:
    pass
