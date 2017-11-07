#!/usr/bin/python
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from struct import unpack

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class MPS7Data(object):
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

    def extract_and_transform(self):
        _file = open(self.file_path, 'rb')
        _bytes = _file.read()

        idx = 9
        while True:
            chunks = get_chunks(_bytes, idx, 1, 4, 8, 8)
            if not chunks:
                break
            record = Record(chunks, idx)
            idx = next_record_at(idx, record.get_kind())
            self.update_stats(record)
            self.records.append(record)

        _file.close()

    def update_stats(self, record):
        user = self.upsert_user(record)
        kind = record.get_kind()

        if kind in ('StartAutopay', 'EndAutopay'):
            self.stats['kindCount'][kind] += 1

        if kind in ('Credit', 'Debit'):
            self.stats['amountTotals'][kind] += record.get_amount()
            user.accumulate_amount(kind, record.get_amount())

    def upsert_user(self, record):
        user_id = str(record.get_user_id())
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

    def unpack_kind(self):
        packed = self.chunks.get('kind')
        return unpack('b', packed)[0] if packed else None

    def unpack_timestamp(self):
        packed = self.chunks.get('timestamp')
        return unpack('>I', packed)[0] if packed else None

    def unpack_user_id(self):
        packed = self.chunks.get('user_id')
        return unpack('>Q', packed)[0] if packed else None

    def unpack_amount(self):
        packed = self.chunks.get('amount')
        return unpack('>d', packed)[0] if packed else None

    def get_kind(self):
        _kind = self.unpack_kind()
        return (
            'Debit',
            'Credit',
            'StartAutopay',
            'EndAutopay'
        )[_kind]

    def get_timestamp(self):
        if self.unpack_timestamp():
            return datetime.fromtimestamp(self.unpack_timestamp())

    def get_user_id(self):
        return self.unpack_user_id()

    def get_amount(self):
        amount = self.unpack_amount()
        return float_to_currency(amount) if amount else None


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
        record.get_kind().ljust(13),
        record.get_timestamp(),
        str(record.get_user_id()).ljust(20),
        str(record.get_amount()).rjust(6)
    )
    return result


def main(show_table=True):
    obj = MPS7Data('data.dat')
    obj.extract_and_transform()

    if show_table:
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
    main()