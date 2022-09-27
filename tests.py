import unittest

from tasks import DataFetchingTask
from utils import CITIES


class MyTest(unittest.TestCase):
    def test_data_fetch(self):
        data = DataFetchingTask(CITIES).get_data()
        print(data)
        info_tzinfo_abbr = data['MOSCOW']['info']['tzinfo']['abbr']
        self.assertEqual(info_tzinfo_abbr, 'MSK')

if __name__ == "__main__":
    unittest.main()
