import unittest

from tasks import DataFetchingTask, DataCalculationTask
from utils import CITIES


class MyTest(unittest.TestCase):

    def test_data_fetch(self):
        data = DataFetchingTask(CITIES).get_data()
        info_tzinfo_abbr = data['MOSCOW']['info']['tzinfo']['abbr']
        self.assertEqual(info_tzinfo_abbr, 'MSK')

    def test_data_calculations(self):
        data = DataFetchingTask(CITIES).get_data()
        obj = DataCalculationTask(data, '2022-05-26', '2022-05-29')




if __name__ == "__main__":
    unittest.main()
