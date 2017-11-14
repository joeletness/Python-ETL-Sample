#!/usr/bin/python
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from struct import unpack

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class MPS7(object):
    def __init__(self, file_name):
        self.data_length = 0
        self.error = ''
        self.log_entries = []
        self.users = {}
        self.file_path = os.path.join(BASE_DIR, file_name)
        self.aggregate = {
            'autopayCount': {
                'StartAutopay': 0,
                'EndAutopay': 0
            },
            'amountTotals': {
                'Debit': float_to_currency(0.0),
                'Credit': float_to_currency(0.0),
            }
        }
        self._extract_transform_load()

    def _extract_transform_load(self):
        open_file = open(self.file_path, 'rb')
        bytes_ = open_file.read()
        open_file.close()

        check_magic_byte(bytes_)
        self.data_length = get_data_length(bytes_)
        
        first_byte_of_logs = 9

        count = 0
        start_byte = first_byte_of_logs
        while True:
            chunks = get_chunks(bytes_, start_byte, 1, 4, 8, 8)
            if not chunks:
                break

            count += 1
            log_entry = LogEntry(chunks, start_byte)
            start_byte = next_log_entry_at(start_byte, log_entry.kind)
            if count <= self.data_length:
                self.update_aggregate(log_entry)
                self.log_entries.append(log_entry)

        if count > self.data_length:
            error_template = 'Expected length to be {}. Actual length {}. Dropping overrun.'
            self.error = error_template.format(self.data_length, count)

    def update_aggregate(self, log_entry):
        user = self.upsert_user(log_entry)
        kind = log_entry.kind

        if kind in ('StartAutopay', 'EndAutopay'):
            self.aggregate['autopayCount'][kind] += 1

        if kind in ('Credit', 'Debit'):
            self.aggregate['amountTotals'][kind] += log_entry.amount
            user.accumulate_amount(kind, log_entry.amount)

    def upsert_user(self, log_entry):
        user_id_ = str(log_entry.user_id)
        user = self.users.get(user_id_)
        if not user:
            self.users[user_id_] = user = User(user_id_)
        return user


class LogEntry(object):
    def __init__(self, chunks=None, index=None):
        self.index = index
        self.chunks = {
                'kind': chunks[0],
                'timestamp': chunks[1],
                'user_id': chunks[2],
                'amount': chunks[3],
            } if chunks else {}

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


def check_magic_byte(bytes_):
    magic_byte = ''.join(unpack('4c', bytes_[0:4]))
    if magic_byte != 'MPS7':
        raise NotMPS7Error


def get_data_length(bytes_):
    return unpack('>I', bytes_[5:9])[0]


def get_chunks(_bytes, start, *args):
    result = []
    for index, size in enumerate(args):
        end = start + size
        chunk = _bytes[start:end]
        if not chunk:
            return []
        result.append(chunk)
        start += size
    return result


def next_log_entry_at(current_position, log_entry_kind):
    if log_entry_kind in ('Credit', 'Debit'):
        return current_position + 21
    return current_position + 13


def float_to_currency(value):
    return Decimal(Decimal(value).quantize(Decimal('.00'), rounding=ROUND_HALF_EVEN))


def format_readable_data_row(log_entry):
    template = '{} | {} | {} | {} | {}'
    result = template.format(
        str(log_entry.index).rjust(5),
        log_entry.kind.ljust(13),
        log_entry.timestamp,
        str(log_entry.user_id).ljust(20),
        str(log_entry.amount).rjust(6)
    )
    return result


class NotMPS7Error(Exception):
    pass


def main(file_name, _user_id=None):
    obj = None
    try:
        obj = MPS7(file_name)
    except NotMPS7Error:
        print 'ERROR ++++ Given data must be "MPS7" ++++'
    except IOError:
        print 'ERROR ++++ File "{}" cannot be opened ++++'.format(file_name)

    if obj:
        print '---------------------------------------------------------------------------'
        if _user_id:
            user = obj.users.get(_user_id)
            print 'Balance for User {} is ${}'.format(_user_id, user.current_balance)
        else:
            print 'byte  | kind          | timestamp           | user_id              | amt'
            print '---------------------------------------------------------------------------'
            for log_entry in obj.log_entries:
                print format_readable_data_row(log_entry)

            print '---------------------------------------------------------------------------'
            print '   Total debit amount | ${}'.format(obj.aggregate['amountTotals']['Debit'])
            print '  Total credit amount | ${}'.format(obj.aggregate['amountTotals']['Credit'])
            print 'Total autopay started | {}'.format(obj.aggregate['autopayCount']['StartAutopay'])
            print '  Total autopay ended | {}'.format(obj.aggregate['autopayCount']['EndAutopay'])

            if obj.error:
                print '---------------------------------------------------------------------------'
                print '!!! ERROR !!! {}'.format(obj.error)
        print '---------------------------------------------------------------------------'


if __name__ == '__main__':
    filename = os.sys.argv[1] if len(os.sys.argv) > 1 else None
    user_id = os.sys.argv[2] if len(os.sys.argv) > 2 else None
    assert filename, 'Data filename is required'
    main(filename, user_id)
