#!/usr/bin/python
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN, getcontext, ROUND_HALF_DOWN, InvalidOperation
import struct
from struct import unpack

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


class MPS7Object(object):
    def __init__(self, *args, **kwargs):
        self.format = None
        self.version = None
        self.count = None
        self.records = []
        self.file_path = get_file_path(args[0]) if args else None

    def read_records(self):
        print ''
        _file = open(self.file_path, 'rb')
        _bytes = _file.read()
        for idx, _byte in enumerate(_bytes):
            if idx >= 9:
                value = struct.unpack('b', _byte)[0]
                if value in (0, 1, 2, 3):
                    record = Record(get_chunks(_bytes, idx, 1, 4, 8, 8), idx)
                    self.records.append(record)
        _file.close()
        return self

    def read_records_alt(self):
        _file = open(self.file_path, 'rb')
        _bytes = _file.read()

        header = get_chunks(_bytes, 0, 4, 1, 4)
        print ''.join(unpack('cccc', header[0]))
        print unpack('b', header[1])[0]
        print unpack('>I', header[2])[0]

        idx = 9
        while True:
            chunks = get_chunks(_bytes, idx, 1, 4, 8, 8)
            if not chunks:
                break
            record = Record(chunks, idx)
            idx = next_record_at(idx, record.get_kind())
            self.records.append(record)
        _file.close()
        return self

    @staticmethod
    def format_list_row(record):
        print type(record.unpack_amount())
        template = '{} | {} | {} | {} | {}'
        result = template.format(
            str(record.index).rjust(5),
            record.get_kind().ljust(13),
            record.get_timestamp(),
            record.get_user_id(),
            record.get_amount()
        )
        return result


def next_record_at(current_position, record_kind):
    if record_kind in ('Credit', 'Debit'):
        return current_position + 21
    return current_position + 13


class Record(object):
    def __init__(self, chunks=None, index=None):
        self.index = index
        self._kind = chunks[0] if chunks else None
        self._timestamp = chunks[1] if chunks else None
        self._user_id = chunks[2] if chunks else None
        self._amount = chunks[3] if chunks else None

        if chunks:
            self.chunks = {
                'kind': chunks[0],
                'timestamp': chunks[1],
                'user_id': chunks[2],
                'amount': chunks[3],
            }
        else:
            self.chunks = {}

        self.kind = None
        self.timestamp = None
        self.user_id = None
        self.amount = None

    def get_timestamp(self):
        if self.unpack_timestamp():
            return datetime.fromtimestamp(self.unpack_timestamp())

    def get_kind(self):
        _kind = self.unpack_kind()
        if _kind is None:
            _kind = self.kind
        return (
            'Debit',
            'Credit',
            'StartAutopay',
            'EndAutopay'
        )[_kind]

    def get_amount(self):
        if self.amount is not None:
            return self.amount
        amount = self.unpack_amount()
        return float_to_currency(amount) if amount else None

    def get_user_id(self):
        return self.unpack_user_id()

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


def get_file_path(file_name):
    return os.path.join(BASE_DIR, file_name)


def get_chunks(_bytes, start, *args):
    result = []
    for size in args:
        end = start + size
        _byte = _bytes[start:end]
        if not _byte:
            return []
        result.append(_byte)
        start += size
    return result


def float_to_currency(value):
    return Decimal(Decimal(value).quantize(Decimal('.00'), rounding=ROUND_HALF_EVEN))


def parse_data_spike(obj):
    _file = open(obj.file_path, 'rb')
    _bytes = _file.read()
    obj.records = []

    print '--------------DATA READ FROM FILE ----------------'
    for idx, _byte in enumerate(_bytes):
        value = struct.unpack('b', _byte)[0]
        if value in (0, 1, 2, 3):
            rs = get_chunks(_bytes, idx, 1, 4, 8, 8)
            record = Record(rs)
            record.kind = unpack('b', rs[0])[0]
            record.timestamp = datetime.fromtimestamp(unpack('>I', rs[1])[0])
            record.user_id = unpack('>Q', rs[2])[0]
            record.amount = round(unpack('>d', rs[3])[0], 8)

            _record_array = [
                unpack('b', rs[0])[0],
                datetime.fromtimestamp(unpack('>I', rs[1])[0]),
                unpack('>Q', rs[2])[0]
            ]
            if value in (0, 1):
                amount = round(unpack('>d', rs[3])[0], 2)
                _record_array.append(amount)

            # print str(idx).rjust(5), ' ', record.get_kind().ljust(13), record.timestamp, ' ', str(record.user_id).ljust(20), record.get_amount()
            obj.records.append(record)

    _file.close()

    return obj