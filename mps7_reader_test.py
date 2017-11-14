from unittest import TestCase
import pytest
import time
from struct import pack
from mps7_reader import *

FIXTURE_FILENAME = 'data.dat'


class TestMPS7Integration(TestCase):
    def test__extract_transform_load__when_file_is_not_found__raise_exception(self):
        with pytest.raises(IOError):
            MPS7('not-found.dat')

    def test__extract_transform_load__when_data_is_mps7__read_data_populate_log_entries_and_users(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert obj.data_length == 71
        assert len(obj.users) == 62

    def test__extract_transform_load__when_length_is_overrun__record_error_and_drop_overrun(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert obj.errors[0] == 'Error: Expected length to be 71. Actual length 72. Dropping overrun.'
        assert len(obj.log_entries) == 71

    def test_update_aggregate__counts_occurrences_of_start_and_end_autopay_log_entries(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert obj.aggregate['autopayCount']['StartAutopay'] == 10
        assert obj.aggregate['autopayCount']['EndAutopay'] == 8

    def test_update_aggregate__accumulates_credits(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert obj.aggregate['amountTotals']['Credit'] == Decimal('9366.00')

    def test_update_aggregate__accumulates_debits(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert obj.aggregate['amountTotals']['Debit'] == Decimal('18203.69')

    def test_update_aggregate__accumulates_users_debits_and_credits(self):
        obj = MPS7(FIXTURE_FILENAME)
        user = obj.users.get('3018469034978866138')
        assert user.current_balance == Decimal('154.66')

    def test_upsert_user__inserts_user_object_in_stats(self):
        obj = MPS7(FIXTURE_FILENAME)
        assert isinstance(obj.users.get('3018469034978866138'), User)


class TestLogEntry(TestCase):
    def test__when_constructed_with_given_chunks__populates_chunk_dictionary(self):
        chunks = ['kind-chunk', 'timestamp-chunk', 'user_id-chunk','amount-chunk']
        log_entry = LogEntry(chunks)
        assert log_entry.chunks['kind'] == 'kind-chunk'
        assert log_entry.chunks['timestamp'] == 'timestamp-chunk'
        assert log_entry.chunks['user_id'] == 'user_id-chunk'
        assert log_entry.chunks['amount'] == 'amount-chunk'

    def test_kind_property__return_readable_value(self):
        log_entry = LogEntry()
        log_entry.chunks['kind'] = pack('b', 0)
        assert log_entry.kind == 'Debit'

    def test_timestamp_property__return_expected_datetime_object(self):
        expected = datetime(2017, 11, 5)
        fake_timestamp_int = int(time.mktime(expected.timetuple()))
        log_entry = LogEntry()
        log_entry.chunks['timestamp'] = pack('>I', fake_timestamp_int)
        assert log_entry.timestamp == expected

    def test_user_id_property__return_user_id_int(self):
        log_entry = LogEntry()
        log_entry.chunks['user_id'] = pack('>Q', 2456938384156277127)
        assert log_entry.user_id == 2456938384156277127

    def test_amount_property__return_decimal_value(self):
        log_entry = LogEntry()
        log_entry.chunks['kind'] = pack('b', 1)
        log_entry.chunks['amount'] = pack('>d', 42.4)
        assert log_entry.amount == Decimal('42.40')


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


def test_check_magic_byte__when_not_msp7__raise_execption():
    fake_bytes = 'wrng'
    with pytest.raises(NotMPS7Error):
        check_magic_byte(fake_bytes)


def test_check_magic_byte__when_msp7__do_nothing():
    fake_bytes = pack('4c', 'M', 'P', 'S', '7')
    check_magic_byte(fake_bytes)


def test_get_data_length():
    fake_bytes = pack('4cbI', 'M', 'P', 'S', '7', 1, 25)
    assert get_data_length(fake_bytes) == 25


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


def test_next_offest__when_debit_or_credit__return_location_of_next_log_entry_21_bytes_ahead():
    assert next_log_entry_at(30, 'Credit') == 51
    assert next_log_entry_at(30, 'Debit') == 51


def test_next_offest__when_not_debit_or_credit__return_location_of_next_log_entry_13_bytes_ahead():
    assert next_log_entry_at(30, 'Not Credit') == 43


def test_float_to_currency__converts_float_to_decimal_with_two_places():
    assert float_to_currency(384.61670768) == Decimal('384.62')


def test_format_readable_data_row__return_readable_data_row_for_output():
    obj = MPS7(FIXTURE_FILENAME)
    result = format_readable_data_row(obj.log_entries[0])
    expected = '    9 | Debit         | 2014-02-22 16:42:25 | 4136353673894269217  | 604.27'
    assert result == expected
