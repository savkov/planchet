import json
import os

import pandas as pd
import pytest

from planchet.io import CsvReader, JsonlReader, CsvWriter, JsonlWriter


@pytest.mark.parametrize(
    "config",
    [
        {
            'csv_text': 'head1,head2\nval1,val2\nval3,val4',
            'batch_size': 2,
            'exception': None

        },
        {
            'csv_text': 'head1,head2\nval1,val2\nval3,val4',
            'batch_size': 10,
            'exception': None

        },
        {
            'csv_text': 'head1,head2\nval1,val2\nval3,val4',
            'batch_size': 0,
            'exception': None

        },
        {
            'csv_text': 'head1,head2\nval1,val2\nval3,val4',
            'batch_size': 1,
            'exception': None

        },
        {
            'csv_text': 'head1,head2\n',
            'batch_size': 1,
            'exception': None

        },
        {
            'csv_text': 'head1,head2\nval1,val2\nval3,val4',
            'batch_size': 4,
            'exception': FileNotFoundError,
            'file_path': 'bad_file_name.csv'
        },
        {
            'csv_text': 'some bad csv text',
            'batch_size': 4,
            'exception': None
        },
    ],
)
def test_csv_read(config):
    file_path = 'temp.csv'
    metadata = {
        'input_file_path': config.get('file_path', file_path),
    }
    with open(file_path, 'w') as fh:
        fh.write(config['csv_text'])

    thrown = False
    try:
        reader = CsvReader(metadata)
        reader(2)
    except config['exception'] as e:
        print(f'{e}')
        thrown = True
    assert thrown == bool(config['exception'])

    os.remove(file_path)


@pytest.mark.parametrize(
    "config",
    [
        {
            'jsonl': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}',
            'batch_size': 2,
            'exception': None

        },
        {
            'jsonl': '{"k1": "v1", "k2": "v2"\n{"k1": "v11", "k2": "v22"}',
            'batch_size': 2,
            'exception': None

        },
        {
            'jsonl': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}',
            'batch_size': 10,
            'exception': None

        },
        {
            'jsonl': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}',
            'batch_size': 2,
            'exception': FileNotFoundError,
            'file_path': 'bad_file.jsonl'

        },
    ],
)
def test_jsnl_read(config):
    file_path = 'temp.jsnl'
    metadata = {
        'input_file_path': config.get('file_path', file_path),
    }
    with open(file_path, 'w') as fh:
        fh.write(config['jsonl'])

    thrown = None
    try:
        reader = JsonlReader(metadata)
        reader(2)
    except config['exception'] as e:
        print(f'Thrown exception message: {e}')
        thrown = type(e)
    assert thrown == config['exception'], \
        f'{thrown} triggered, expected {config["exception"]}'

    os.remove(file_path)


@pytest.mark.parametrize(
    "config",
    [
        {
            'file': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}\n',
            'data': [{"k1": "v5", "k2": "v6"}, {"k1": "v7", "k2": "v8"}],
            'batch_size': 2,
            'exception': FileNotFoundError,
            'overwrite': False,
            'file_path': 'fake-dir-ljfgndflgjndf/bad-file.csv'
        },
        {
            'file': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}\n',
            'data': [{"k1": "v5", "k2": "v6"}, {"k1": "v7", "k2": "v8"}],
            'batch_size': 2,
            'exception': None,
            'overwrite': False
        },
        {
            'file': '',
            'data': [{"k1": "v1", "k2": "v2"}, {"k1": "v11", "k2": "v22"}],
            'batch_size': 2,
            'exception': None,
            'overwrite': False
        },
        {
            'file': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}\n',
            'data': [],
            'batch_size': 2,
            'exception': None,
            'overwrite': False
        },
        {
            'file': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}\n',
            'data': [{"k1": "v5", "k2": "v6"}, {"k1": "v7", "k2": "v8"}],
            'batch_size': 2,
            'exception': None,
            'overwrite': True
        },
        {
            'file': '',
            'data': [{"k1": "v1", "k2": "v2"}, {"k1": "v11", "k2": "v22"}],
            'batch_size': 2,
            'exception': None,
            'overwrite': True
        },
        {
            'file': '{"k1": "v1", "k2": "v2"}\n{"k1": "v11", "k2": "v22"}\n',
            'data': [],
            'batch_size': 2,
            'exception': None,
            'overwrite': True
        },
    ],
)
def test_jsnl_write(config):
    file_path = 'temp.jsnl'
    metadata = {
        'output_file_path': config.get('file_path', file_path),
        'overwrite': config.get('overwrite', False)
    }

    file_string = config['file']
    data_string = '\n'.join([json.dumps(jsn) for jsn in config['data']])

    with open(file_path, 'w') as fh:
        fh.write(file_string)

    try:
        writer = JsonlWriter(metadata)
        writer(config['data'])
        with open(file_path) as fh:
            results_string = fh.read()
    except config['exception'] as e:
        print(f'Thrown exception message: {e}')
        assert type(e) == config['exception'], \
            f'{type(e)} triggered, expected {config["exception"]}'
        os.remove(file_path)
        return
    assert config.get('exception') is None
    if config['overwrite']:
        assert results_string == data_string + '\n', 'Overwriting error'
    else:
        assert results_string == (file_string + data_string + '\n').lstrip()
    os.remove(file_path)


@pytest.mark.parametrize(
    "config",
    [
        {
            'file': 'head1,head2\nval1,val2\nval3,val4\n',
            'data': [{'head1': 'val11', 'head2': 'val33'}],
            'batch_size': 2,
            'exception': FileNotFoundError,
            'file_path': 'fake-dir-slfkf/bad-file.csv'
        },
        {
            'file': 'head1,head2\nval1,val2\nval3,val4\n',
            'data': [],
            'batch_size': 2,
        },
        {
            'file': 'head1,head2\nval1,val2\nval3,val4\n',
            'data': [{'head1': 'val11', 'head2': 'val33'}],
            'batch_size': 2,
        },
        {
            'file': 'head1,head2\nval1,val2\nval3,val4\n',
            'data': [],
            'batch_size': 2,
            'overwrite': True
        },
        {
            'file': 'head1,head2\nval1,val2\nval3,val4\n',
            'data': [{'head1': 'val11', 'head2': 'val33'}],
            'batch_size': 2,
            'overwrite': True
        },
    ],
)
def test_csv_read_advanced(config):
    file_path = 'temp.csv'
    metadata = {
        'output_file_path': config.get('file_path', file_path),
        'overwrite': config.get('overwrite', False)
    }
    try:
        with open(file_path, 'w') as fh:
            fh.write(config['file'])
        file_csv = pd.read_csv(file_path)
        data_csv = pd.DataFrame(config['data'])
        writer = CsvWriter(metadata)
        writer(config['data'])
        result_csv = pd.read_csv(file_path)
    except config.get('exception') as e:
        print(f'{e}')
        assert type(e) == config['exception']
        os.remove(file_path)
        return
    assert config.get('exception') is None
    concat = pd.concat([file_csv, data_csv])
    for (_, row1), (_, row2) in zip(result_csv.iterrows(), concat.iterrows()):
        assert row1.to_dict() == row2.to_dict()
    os.remove(file_path)


@pytest.mark.parametrize('batch_size', [1, 2, 5, 10, 13, 30, 32])
def test_csv_read_batch(reader, batch_size):
    items = reader(batch_size)
    n_items = batch_size if batch_size < 30 else 30
    assert len(items) == n_items
