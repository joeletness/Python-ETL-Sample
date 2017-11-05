from unittest import TestCase
import time
from datetime import datetime
from struct import pack
from decimal import Decimal
from byte_reader import MPS7Object, Record, parse_data_spike, get_chunks, float_to_currency
from byte_reader import next_record_at


def test_get_chunks__when_passed_blob_with_start_and_chunk_size__returns_chunks():
    blob = 'pearcowtomatotunabeeorange'
    start_index = 0
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == ['pear', 'cow', 'tomato']

    start_index = 13
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == ['tuna', 'bee', 'orange']


def test_get_chunks__when_start_index_out_of_range__returns_none():
    blob = 'pearcowtomatotunabeeorange'
    start_index = 45
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == []


def test_parse_data_spike():
    obj = MPS7Object('data.dat')
    parse_data_spike(obj)
    assert len(obj.records) == 92
    assert obj.records[0].chunks
    assert 'Credit' in obj.records[0].get_kind()
    print


def test_float_to_currency__converts_double_to_decimal_with_two_places():
    assert str(float_to_currency(384.61670768)) == '384.62'
    assert float_to_currency(384.61670768) == Decimal('384.62')
    foo = float_to_currency(384.61670768)


def test_next_offest__when_debit_or_credit__return_location_of_next_record_21_bytes_ahead():
    assert next_record_at(30, 'Credit') == 51
    assert next_record_at(30, 'Debit') == 51


def test_next_offest__when_not_debit_or_credit__return_location_of_next_record_13_bytes_ahead():
    assert next_record_at(30, 'Not Credit') == 43


class TestMPS7Object(TestCase):
    def test_read_records(self):
        obj = MPS7Object('data.dat')
        obj.read_records()
        assert len(obj.records) == 88

    def test_read_records_alt(self):
        obj = MPS7Object('data.dat')
        obj.read_records_alt()
        assert len(obj.records) == 72

    # def test_set_header(self):
    #     obj = MPS7Object('data.dat')
    #     obj.read_records_alt()
    #     assert obj.count == 71

    def test_format_list_row(self):

        # 9   Debit         2014-02-22 16:42:25   4136353673894269217  604.27433556

        print_he = ' byte | kind          | timestamp           | user_id             | amt'
        expected = '    9 | Debit         | 2014-02-22 16:42:25 | 4136353673894269217 | 604.27'
        obj = MPS7Object('data.dat')
        obj.read_records()

        result = obj.format_list_row(obj.records[0])
        assert result == expected


class TestRecord(TestCase):
    def test_new_instance__when_positional_chunks_are_given__sets_dictionary(self):
        chunks = ['kind-byte', 'timestamp-bytes', 'user_id-bytes','amount-bytes']
        record = Record(chunks)
        assert record.chunks['kind'] == 'kind-byte'
        assert record._timestamp == 'timestamp-bytes'
        assert record._user_id == 'user_id-bytes'
        assert record._amount == 'amount-bytes'

    def test_unpack_kind__returns_unpacked_value(self):
        record = Record()
        record.chunks['kind'] = pack('b', 2)
        assert record.unpack_kind() == 2

    def test_unpack_timestamp__returns_unpacked_value(self):
        fake_timestamp_int = int(time.mktime(datetime.now().timetuple()))
        record = Record()
        record.chunks['timestamp'] = pack('>I', fake_timestamp_int)
        assert record.unpack_timestamp() == fake_timestamp_int

    def test_unpack_user_id__returns_unpacked_value(self):
        record = Record()
        record.chunks['user_id'] = pack('>Q', 2456938384156277127)
        assert record.unpack_user_id() == 2456938384156277127

    def test_unpack_amount__returns_unpacked_value(self):
        record = Record()
        record.chunks['amount'] = pack('>d', 42.473264)
        assert record.unpack_amount() == 42.473264

    def test_get_kind__returns_readable_value(self):
        record = Record()
        record.chunks['kind'] = pack('b', 0)
        assert 'Debit' in record.get_kind()

    def test_get_timestamp__when_no_data__returns_none(self):
        record = Record()
        assert record.get_timestamp() is None

    def test_get_timestamp__when_data__returns_expected_timestamp(self):
        expected = datetime(2017, 11, 5)
        fake_timestamp_int = int(time.mktime(expected.timetuple()))
        record = Record()
        record.chunks['timestamp'] = pack('>I', fake_timestamp_int)
        assert record.get_timestamp() == expected

    def test_get_user_id__when_no_data__return_none(self):
        record = Record()
        record.chunks['user_id'] = pack('>Q', 2456938384156277127)
        assert record.get_user_id() == 2456938384156277127

    def test_get_amount__when_no_data__return_none(self):
        record = Record()
        assert record.get_amount() is None

    def test_get_amount__when_data__return_decimal_value(self):
        record = Record()
        record.chunks['kind'] = pack('b', 1)
        record.chunks['amount'] = pack('>d', 42.4)
        assert record.get_amount() == Decimal('42.40')
