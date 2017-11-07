#!/usr/bin/python
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from struct import unpack

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class MPS7(object):
    def __init__(self, file_name):
        self.records = []
        self.users = {}
        self.file_path = os.path.join(BASE_DIR, file_name)
        self.stats = {
            'kindCount': {
                'StartAutopay': 0,
                'EndAutopay': 0
            },
            'amountTotals': {
                'Debit': float_to_currency(0.0),
                'Credit': float_to_currency(0.0),
            }
        }
        self._extract_and_transform()

    def _extract_and_transform(self):
        open_file = open(self.file_path, 'rb')
        bytes_ = open_file.read()
        data_format = ''.join(unpack('4c', bytes_[0:4]))
        assert data_format == 'MPS7', 'Data must be MPS7'

        idx = 9
        while True:
            chunks = get_chunks(bytes_, idx, 1, 4, 8, 8)
            if not chunks:
                break
            record = Record(chunks, idx)
            idx = next_record_at(idx, record.kind)
            self.update_stats(record)
            self.records.append(record)

        open_file.close()

    def update_stats(self, record):
        user = self.upsert_user(record)
        kind = record.kind

        if kind in ('StartAutopay', 'EndAutopay'):
            self.stats['kindCount'][kind] += 1

        if kind in ('Credit', 'Debit'):
            self.stats['amountTotals'][kind] += record.amount
            user.accumulate_amount(kind, record.amount)

    def upsert_user(self, record):
        user_id = str(record.user_id)
        user = self.users.get(user_id)
        if not user:
            self.users[user_id] = user = User(user_id)
        return user


class Record(object):
    def __init__(self, chunks=None, index=None):
        self.index = index
        if chunks:
            self.chunks = {
                'kind': chunks[0],
                'timestamp': chunks[1],
                'user_id': chunks[2],
                'amount': chunks[3],
            }
        else:
            self.chunks = {}

    @property
    def kind(self):
        kind = unpack('b', self.chunks.get('kind'))[0]
        return (
            'Debit',
            'Credit',
            'StartAutopay',
            'EndAutopay'
        )[kind]

    @property
    def timestamp(self):
        unpacked = unpack('>I', self.chunks.get('timestamp'))[0]
        return datetime.fromtimestamp(unpacked)

    @property
    def user_id(self):
        packed = self.chunks.get('user_id')
        return unpack('>Q', packed)[0] if packed else None

    @property
    def amount(self):
        unpacked = unpack('>d', self.chunks.get('amount'))[0]
        return float_to_currency(unpacked)


class User(object):
    def __init__(self, user_id):
        self.user_id = user_id
        self.credit_sum = float_to_currency(0.0)
        self.debit_sum = float_to_currency(0.0)

    def accumulate_amount(self, kind, amount):
        if kind == 'Credit':
            self.credit_sum += amount
        elif kind == 'Debit':
            self.debit_sum += amount

    @property
    def current_balance(self):
        return self.credit_sum - self.debit_sum


def get_chunks(_bytes, start, *args):
    result = []
    for index, size in enumerate(args):
        end = start + size
        _byte = _bytes[start:end]
        if not _byte:
            return []
        result.append(_byte)
        start += size
    return result


def next_record_at(current_position, record_kind):
    if record_kind in ('Credit', 'Debit'):
        return current_position + 21
    return current_position + 13


def float_to_currency(value):
    return Decimal(Decimal(value).quantize(Decimal('.00'), rounding=ROUND_HALF_EVEN))


def format_readable_data_row(record):
    template = '{} | {} | {} | {} | {}'
    result = template.format(
        str(record.index).rjust(5),
        record.kind.ljust(13),
        record.timestamp,
        str(record.user_id).ljust(20),
        str(record.amount).rjust(6)
    )
    return result


def main(filename):
    obj = MPS7(filename)

    print '---------------------------------------------------------------------------'
    print 'byte  | kind          | timestamp           | user_id              | amt'
    print '---------------------------------------------------------------------------'
    for record in obj.records:
        print format_readable_data_row(record)

    print '---------------------------------------------------------------------------'
    print '   Total debit amount | ${}'.format(obj.stats['amountTotals']['Debit'])
    print '  Total credit amount | ${}'.format(obj.stats['amountTotals']['Credit'])
    print 'Total autopay started | {}'.format(obj.stats['kindCount']['StartAutopay'])
    print '  Total autopay ended | {}'.format(obj.stats['kindCount']['EndAutopay'])
    print '---------------------------------------------------------------------------'

    user = obj.users.get('2456938384156277127')
    print 'Balance for User 2456938384156277127 is ${}'.format(user.current_balance)


if __name__ == '__main__':
    filename = os.sys.argv[1] if len(os.sys.argv) > 1 else None
    assert filename, 'Data filename is required'
    main(filename)
