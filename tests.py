import unittest
import os
import json
import io
import sys

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask
)
from utils import CITIES, find_file
from statistics import mean


class MyTest(unittest.TestCase):

    def test_data_fetch(self):
        data = DataFetchingTask(CITIES).get_data()
        info_tzinfo_abbr = data['MOSCOW']['info']['tzinfo']['abbr']
        self.assertEqual(info_tzinfo_abbr, 'MSK')

    def test_data_calculations(self):
        data = DataFetchingTask(CITIES).get_data()
        data_for_city = data['MOSCOW']
        data_calc_obj = DataCalculationTask('2022-05-26', '2022-05-29')
        days = data_calc_obj._get_days_period(data_for_city)
        filtered_data_for_days = data_calc_obj._get_filtered_dates_data(data_for_city)
        hours = data_calc_obj._get_period_of_hours(days[0])
        date = filtered_data_for_days['26-05']
        hour = date[0]
        avg_temp = round(mean([data_calc_obj._get_temp(hour) for hour in date]))
        avg_cond = data_calc_obj._get_avg_date_cond(date)
        date_data_w_avg = data_calc_obj._calculate_data_for_date(date)
        dates_with_avg = {
            '26-05': {'avg_temp': 14, 'cond_hours': 3},
            '27-05': {'avg_temp': 16, 'cond_hours': 5}
        }
        data_for_city = data_calc_obj._get_params_for_city(dates_with_avg)
        result_data = DataCalculationTask('2022-05-26', '2022-05-29').calculate_data(data)

        self.assertTrue(days)
        self.assertTrue(filtered_data_for_days)
        self.assertEqual(data_calc_obj._get_formatted_date(days[0]), '26-05')
        self.assertEqual(len(days), 4)
        self.assertEqual(len(hours), 11)
        self.assertEqual(len(filtered_data_for_days), 4)
        self.assertIn('26-05', filtered_data_for_days)
        self.assertTrue(date)
        self.assertTrue(hour)
        self.assertTrue(data_calc_obj._get_temp(hour))
        self.assertEqual(data_calc_obj._get_avg_date_temp(date), avg_temp)
        self.assertTrue(data_calc_obj._get_condition(hour))
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
        data = DataCalculationTask('2022-05-26', '2022-05-29').calculate_data(
            DataFetchingTask(CITIES).get_data()
        )
        file_name = 'test.json'
        data_agr_obj = DataAggregationTask(file_name)
        renamed_data = data_agr_obj._get_renamed_dict(data)
        with open(file_name, 'w'):
            pass
        data_agr_obj.write_data_to_file(data)
        with open(file_name) as file:
            data_in_file = json.load(file)
        self.assertTrue('Москва' in renamed_data)
        self.assertTrue('День' in renamed_data['Москва'])
        self.assertTrue('26-05' in renamed_data['Москва']['День'])
        self.assertTrue('Температура, среднее' in renamed_data['Москва']['День']['26-05'])
        self.assertTrue('Без осадков, часов' in renamed_data['Москва']['День']['26-05'])
        self.assertTrue('Среднее' in renamed_data['Москва'])
        self.assertTrue('Температура, среднее' in renamed_data['Москва']['Среднее'])
        self.assertTrue('Без осадков, часов' in renamed_data['Москва']['Среднее'])
        self.assertTrue('Рейтинг' in renamed_data['Москва'])
        self.assertEqual(renamed_data, data_in_file)
        self.assertTrue(find_file(file_name).endswith(file_name))
        os.remove(file_name)

    def test_data_analyzing(self):
        data = DataCalculationTask('2022-05-26', '2022-05-29').calculate_data(
            DataFetchingTask(CITIES).get_data()
        )
        file_name = 'test.json'
        data_agr_obj = DataAggregationTask(file_name)
        data_agr_obj.write_data_to_file(data)
        capture = io.StringIO()
        sys.stdout = capture
        data_analyzing_obj = DataAnalyzingTask(file_name)
        data_analyzing_obj.get_best_cities()
        self.assertEqual(capture.getvalue(), 'Лучший город для отдыха это: Каир\n')
        data['ABUDHABI']['rating'] = 1
        data_agr_obj.write_data_to_file(data)
        capture_for_two_cities = io.StringIO()
        sys.stdout = capture_for_two_cities
        data_analyzing_obj.get_best_cities()
        self.assertEqual(
            capture_for_two_cities.getvalue(),
            'Лучшие города для отдыха это: Абу Даби, Каир\n' or 'Лучшие города для отдыха это: Каир, Абу Даби\n')
        os.remove(file_name)


if __name__ == "__main__":
    unittest.main()
