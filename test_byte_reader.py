from unittest import TestCase
from byte_reader import get_data


class TestByteReader(TestCase):
    def test_get_data_reads_a_given_file(self):
        data = get_data('sample.dat')
        self.assertEqual('sampleString',data)
