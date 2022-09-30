import unittest

from tasks import DataFetchingTask, DataCalculationTask
from utils import CITIES
from statistics import mean


class MyTest(unittest.TestCase):

    def test_data_fetch(self):
        data = DataFetchingTask(CITIES).get_data()
        info_tzinfo_abbr = data['MOSCOW']['info']['tzinfo']['abbr']
        self.assertEqual(info_tzinfo_abbr, 'MSK')

    def test_data_calculations(self):
        data = DataFetchingTask(CITIES).get_data()
        data_for_city = data['MOSCOW']
        obj = DataCalculationTask('2022-05-26', '2022-05-29')
        days = obj._get_days_period(data_for_city)
        filtered_data_for_days = obj._get_filtered_dates_data(data_for_city)
        hours = obj._get_period_of_hours(days[0])
        date = filtered_data_for_days['26-05']
        hour = date[0]
        avg_temp = round(mean([obj._get_temp(hour) for hour in date]))
        avg_cond = obj._get_avg_date_cond(date)
        date_data_w_avg = obj._calculate_data_for_date(date)
        dates_with_avg = {
            '26-05': {'avg_temp': 14, 'cond_hours': 3},
            '27-05': {'avg_temp': 16, 'cond_hours': 5}
        }
        data_for_city = obj._get_params_for_city(dates_with_avg)
        result_data = DataCalculationTask('2022-05-26', '2022-05-29').calculate_data(data)

        self.assertTrue(days)
        self.assertTrue(filtered_data_for_days)
        self.assertEqual(obj._get_formatted_date(days[0]), '26-05')
        self.assertEqual(len(days), 4)
        self.assertEqual(len(hours), 11)
        self.assertEqual(len(filtered_data_for_days), 4)
        self.assertIn('26-05', filtered_data_for_days)
        self.assertTrue(date)
        self.assertTrue(hour)
        self.assertTrue(obj._get_temp(hour))
        self.assertEqual(obj._get_avg_date_temp(date), avg_temp)
        self.assertTrue(obj._get_condition(hour))
        self.assertGreater(avg_cond, 0)
        self.assertTrue(date_data_w_avg.get('avg_temp'))
        self.assertTrue(date_data_w_avg.get('cond_hours'))
        self.assertEqual(data_for_city['AVG']['avg_temp'], 15)
        self.assertEqual(data_for_city['AVG']['cond_hours'], 4)
        self.assertEqual(data_for_city['total_score'], 19)
        self.assertEqual(len(result_data), 15)
        self.assertTrue(result_data['MOSCOW'])
        self.assertTrue(result_data['MOSCOW'].get('dates'))
        self.assertTrue(result_data['MOSCOW'].get('AVG'))
        self.assertTrue(result_data['MOSCOW'].get('rating'))
        self.assertEqual(result_data['MOSCOW'].get('rating'), 12)

    def test_data_aggregation(self):
        pass


if __name__ == "__main__":
    unittest.main()
