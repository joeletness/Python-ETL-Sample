from unittest import TestCase
import time
from datetime import datetime
from struct import pack
from decimal import Decimal
from byte_reader import MPS7Data, Record, get_chunks, float_to_currency, User
from byte_reader import next_record_at, main, format_readable_data_row


def test_main():
    main()


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
    start_index = 26
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == []


def test_float_to_currency__converts_float_to_decimal_with_two_places():
    assert str(float_to_currency(384.61670768)) == '384.62'
    assert float_to_currency(384.61670768) == Decimal('384.62')


def test_next_offest__when_debit_or_credit__return_location_of_next_record_21_bytes_ahead():
    assert next_record_at(30, 'Credit') == 51
    assert next_record_at(30, 'Debit') == 51


def test_next_offest__when_not_debit_or_credit__return_location_of_next_record_13_bytes_ahead():
    assert next_record_at(30, 'Not Credit') == 43


def test_format_readable_data_row():
    expected = '    9 | Debit         | 2014-02-22 16:42:25 | 4136353673894269217  | 604.27'
    obj = MPS7Data('data.dat')
    obj.read_data_from_file()
    result = format_readable_data_row(obj.records[0])
    assert result == expected


class TestMPS7Object(TestCase):
    def test_read_data_from_file(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        assert len(obj.records) == 72

    def test_update_stats__counts_occurrences_of_each_record_type(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        assert obj.stats['kindCount']['Debit'] == 36
        assert obj.stats['kindCount']['Credit'] == 18
        assert obj.stats['kindCount']['StartAutopay'] == 10
        assert obj.stats['kindCount']['EndAutopay'] == 8

    def test_update_stats__accumulates_credits(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        assert obj.stats['amountTotals']['Credit'] == Decimal('10073.34')

    def test_update_stats__accumulates_debits(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        assert obj.stats['amountTotals']['Debit'] == Decimal('18203.69')

    def test_update_stats__accumulates_users_debits_and_credits(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        user = obj.stats['users'].get('3018469034978866138')
        assert user.current_balance == Decimal('154.66')

    def test_upsert_user__inserts_user_object_in_stats(self):
        obj = MPS7Data('data.dat')
        obj.read_data_from_file()
        assert isinstance(obj.stats['users']['3018469034978866138'], User)


class TestUser(TestCase):
    def test_user_has_id(self):
        user = User('some-id')
        assert user.user_id == 'some-id'

    def test_accumulate_amount__can_add_to_sum_of_credit(self):
        user = User('foo')
        user.accumulate_amount('Credit', 100)
        assert user.credit_sum == 100
        user.accumulate_amount('Credit', 50)
        assert user.credit_sum == 150

    def test_accumulate_amount__can_add_to_sum_of_Debit(self):
        user = User('foo')
        user.accumulate_amount('Debit', 100)
        assert user.debit_sum == 100
        user.accumulate_amount('Debit', 50)
        assert user.debit_sum == 150

    def test_current_balance__return_user_balance(self):
        user = User('foo')
        user.accumulate_amount('Credit', 100)
        user.accumulate_amount('Debit', 50)
        assert user.current_balance == 50


class TestRecord(TestCase):
    def test_new_instance__when_positional_chunks_are_given__sets_dictionary(self):
        chunks = ['kind-byte', 'timestamp-bytes', 'user_id-bytes','amount-bytes']
        record = Record(chunks)
        assert record.chunks['kind'] == 'kind-byte'
        assert record.chunks['timestamp'] == 'timestamp-bytes'
        assert record.chunks['user_id'] == 'user_id-bytes'
        assert record.chunks['amount'] == 'amount-bytes'

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

    def test_is_transaction__when_record_is_debit_or_credit__return_true(self):
        record = Record()
        record.chunks['kind'] = pack('b', 0)
        assert record.is_transaction is True
        record.chunks['kind'] = pack('b', 1)
        assert record.is_transaction is True

    def test_is_transaction__when_record_auto_payment_update__return_False(self):
        record = Record()
        record.chunks['kind'] = pack('b', 2)
        assert record.is_transaction is False
        record.chunks['kind'] = pack('b', 3)
        assert record.is_transaction is False

