from unittest import TestCase
import time
from datetime import datetime
from struct import pack
from decimal import Decimal
from byte_reader import MPS7Data, Record, get_chunks, float_to_currency, User
from byte_reader import next_record_at, main, format_readable_data_row


class TestMPS7Data(TestCase):
    def test_extract_and_transform__reads_data_creates_new_records_and_users(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        assert len(obj.records) == 72
        assert len(obj.users) == 62

    def test_update_stats__counts_occurrences_of_start_and_end_autopay_records(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        assert obj.stats['kindCount']['StartAutopay'] == 10
        assert obj.stats['kindCount']['EndAutopay'] == 8

    def test_update_stats__accumulates_credits_and_debits(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        assert obj.stats['amountTotals']['Credit'] == Decimal('10073.34')
        assert obj.stats['amountTotals']['Debit'] == Decimal('18203.69')

    def test_update_stats__accumulates_debits(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        assert obj.stats['amountTotals']['Debit'] == Decimal('18203.69')

    def test_update_stats__accumulates_users_debits_and_credits(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        user = obj.users.get('3018469034978866138')
        assert user.current_balance == Decimal('154.66')

    def test_upsert_user__inserts_user_object_in_stats(self):
        obj = MPS7Data('data.dat')
        obj.extract_and_transform()
        assert isinstance(obj.users.get('3018469034978866138'), User)


class TestRecord(TestCase):
    def test__when_constructed_with_given_chunks__populates_chunk_dictionary(self):
        chunks = ['kind-chunk', 'timestamp-chunk', 'user_id-chunk','amount-chunk']
        record = Record(chunks)
        assert record.chunks['kind'] == 'kind-chunk'
        assert record.chunks['timestamp'] == 'timestamp-chunk'
        assert record.chunks['user_id'] == 'user_id-chunk'
        assert record.chunks['amount'] == 'amount-chunk'

    def test_kind_property__return_readable_value(self):
        record = Record()
        record.chunks['kind'] = pack('b', 0)
        assert record.kind == 'Debit'

    def test_timestamp_property__return_expected_datetime_object(self):
        expected = datetime(2017, 11, 5)
        fake_timestamp_int = int(time.mktime(expected.timetuple()))
        record = Record()
        record.chunks['timestamp'] = pack('>I', fake_timestamp_int)
        assert record.timestamp == expected

    def test_user_id_property__return_user_id_int(self):
        record = Record()
        record.chunks['user_id'] = pack('>Q', 2456938384156277127)
        assert record.user_id == 2456938384156277127

    def test_amount_property__return_decimal_value(self):
        record = Record()
        record.chunks['kind'] = pack('b', 1)
        record.chunks['amount'] = pack('>d', 42.4)
        assert record.amount == Decimal('42.40')


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


def test_get_chunks__when_passed_blob_with_start_and_chunk_size__return_chunks():
    blob = 'pearcowtomatotunabeeorange'
    start_index = 0
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == ['pear', 'cow', 'tomato']

    start_index = 13
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == ['tuna', 'bee', 'orange']


def test_get_chunks__when_start_index_out_of_range__return_none():
    blob = 'pearcowtomatotunabeeorange'
    start_index = 26
    result = get_chunks(blob, start_index, 4, 3, 6)
    assert result == []


def test_next_offest__when_debit_or_credit__return_location_of_next_record_21_bytes_ahead():
    assert next_record_at(30, 'Credit') == 51
    assert next_record_at(30, 'Debit') == 51


def test_next_offest__when_not_debit_or_credit__return_location_of_next_record_13_bytes_ahead():
    assert next_record_at(30, 'Not Credit') == 43


def test_float_to_currency__converts_float_to_decimal_with_two_places():
    assert float_to_currency(384.61670768) == Decimal('384.62')


def test_format_readable_data_row():
    expected = '    9 | Debit         | 2014-02-22 16:42:25 | 4136353673894269217  | 604.27'
    obj = MPS7Data('data.dat')
    obj.extract_and_transform()
    result = format_readable_data_row(obj.records[0])
    assert result == expected


def test_main():
    main(True)
