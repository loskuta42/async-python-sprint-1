import io
import os
import sys
import unittest
from multiprocessing import Queue
from statistics import mean
from threading import Lock

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask
)
from utils import CITIES, find_file, get_bad_conditions_from_file


class MyTest(unittest.TestCase):

    def test_utils(self):
        test_file_name = 'test.txt'
        with open(test_file_name, 'w') as file:
            file.write('snow - снег.\n')
            file.write('cloudy — облачно с прояснениями.\n')
            file.write('light-rain — небольшой дождь.\n')
            file.write('overcast — пасмурно.\n')
        file_path = find_file(test_file_name)
        self.assertTrue(file_path.endswith(test_file_name))

        bad_cond = get_bad_conditions_from_file(test_file_name)
        self.assertNotIn('overcast', bad_cond)
        self.assertIn('snow', bad_cond)
        self.assertIn('light-rain', bad_cond)
        os.remove(test_file_name)

    def test_data_fetch(self):
        TEST_CITIES = ['MOSCOW', 'CAIRO', 'ROMA']
        CITIES_FOR_TEST = {city: CITIES[city] for city in TEST_CITIES}
        queue = Queue()
        data_fetching_process = DataFetchingTask(cities=CITIES_FOR_TEST, queue=queue)
        data_fetching_process.start()
        data_fetching_process.join()
        data_for_cities = []
        while (data := queue.get()) is not None:
            data_for_cities.append(data)
        self.assertEqual(len(data_for_cities), 3)
        for city_data in data_for_cities:
            with self.subTest(city_data=city_data):
                self.assertIn(city_data['city_name'], TEST_CITIES)
                self.assertIn('forecasts', city_data)

    def test_data_calculations(self):
        TEST_CITIES = ['MOSCOW', 'CAIRO', 'ROMA']
        CITIES_FOR_TEST = {city: CITIES[city] for city in TEST_CITIES}
        queue = Queue()
        result_queue = Queue()
        data_fetching_process = DataFetchingTask(cities=CITIES_FOR_TEST, queue=queue)
        data_fetching_process.start()
        data_fetching_process.join()
        data_for_cities = []
        while (data := queue.get()) is not None:
            data_for_cities.append(data)
        data_for_city = data_for_cities[0]
        data_calculation_process = DataCalculationTask(
            start_day='2022-05-26',
            finish_day='2022-05-29',
            queue=queue,
            result_queue=result_queue
        )
        days = data_calculation_process._get_days_period(data_for_city)
        hours = data_calculation_process._get_period_of_hours(days[0])
        filtered_data_for_days = data_calculation_process._get_filtered_dates_data(data_for_city)
        date = filtered_data_for_days['dates']['26-05']
        hour = date[0]
        avg_temp = round(mean([data_calculation_process._get_temp(hour) for hour in date]))
        avg_cond = data_calculation_process._get_avg_date_cond(date)
        date_data_w_avg = data_calculation_process._calculate_data_for_date(date)
        dates_with_avg = {
            '26-05': {'avg_temp': 14, 'cond_hours': 3},
            '27-05': {'avg_temp': 16, 'cond_hours': 5}
        }
        data_for_city = data_calculation_process._get_params_for_city(dates_with_avg)

        self.assertTrue(days)
        self.assertTrue(filtered_data_for_days)
        self.assertEqual(data_calculation_process._get_formatted_date(days[0]), '26-05')
        self.assertEqual(len(days), 4)
        self.assertEqual(len(hours), 11)
        self.assertEqual(len(filtered_data_for_days['dates']), 4)
        self.assertIn('26-05', filtered_data_for_days['dates'])
        self.assertTrue(date)
        self.assertTrue(hour)
        self.assertTrue(data_calculation_process._get_temp(hour))
        self.assertEqual(data_calculation_process._get_avg_date_temp(date), avg_temp)
        self.assertTrue(data_calculation_process._get_condition(hour))
        self.assertGreater(avg_cond, 0)
        self.assertTrue(date_data_w_avg.get('avg_temp'))
        self.assertTrue(date_data_w_avg.get('cond_hours'))
        self.assertEqual(data_for_city['AVG']['avg_temp'], 15)
        self.assertEqual(data_for_city['AVG']['cond_hours'], 4)
        self.assertEqual(data_for_city['total_score'], 19)

    def test_fetching_and_calculation_processes(self):
        TEST_CITIES = ['MOSCOW', 'CAIRO', 'ROMA']
        CITIES_FOR_TEST = {city: CITIES[city] for city in TEST_CITIES}
        queue = Queue()
        result_queue = Queue()
        data_fetching_process = DataFetchingTask(cities=CITIES_FOR_TEST, queue=queue)
        data_calculation_process = DataCalculationTask(
            start_day='2022-05-26',
            finish_day='2022-05-29',
            queue=queue,
            result_queue=result_queue
        )
        data_fetching_process.start()
        data_calculation_process.start()
        data_fetching_process.join()
        data_calculation_process.join()
        result_data = result_queue.get()
        self.assertEqual(len(result_data), 3)
        for city_data in result_data:
            with self.subTest(city_data=city_data):
                self.assertTrue(city_data.get('dates'))
                self.assertTrue(city_data.get('city_name'))
                self.assertTrue(city_data.get('AVG'))
                self.assertTrue(city_data.get('rating'))

    def test_data_aggregation(self):
        TEST_CITIES = ['MOSCOW', 'CAIRO', 'ROMA']
        CITIES_FOR_TEST = {city: CITIES[city] for city in TEST_CITIES}
        queue = Queue()
        result_queue = Queue()
        data_fetching_process = DataFetchingTask(cities=CITIES_FOR_TEST, queue=queue)
        data_calculation_process = DataCalculationTask(
            start_day='2022-05-26',
            finish_day='2022-05-29',
            queue=queue,
            result_queue=result_queue
        )
        lock = Lock()
        out_file_name = 'test.json'
        data_fetching_process.start()
        data_calculation_process.start()
        data_fetching_process.join()
        data_calculation_process.join()
        result_data = result_queue.get()

        data_aggregation_thread = DataAggregationTask(
            lock=lock,
            file_name=out_file_name,
            results_of_calculations=result_data
        )
        renamed_data = data_aggregation_thread._get_renamed_dict(result_data[0])
        self.assertIn('Город', renamed_data)
        self.assertIn('День', renamed_data)
        self.assertIn('26-05', renamed_data['День'])
        self.assertIn('Температура, среднее', renamed_data['День']['26-05'])
        self.assertIn('Без осадков, часов', renamed_data['День']['26-05'])
        self.assertIn('Среднее', renamed_data)
        self.assertIn('Температура, среднее', renamed_data['Среднее'])
        self.assertIn('Без осадков, часов', renamed_data['Среднее'])
        self.assertIn('Рейтинг', renamed_data)

    def test_data_aggregation_and_analyzing(self):
        TEST_CITIES = ['MOSCOW', 'CAIRO', 'ROMA']
        CITIES_FOR_TEST = {city: CITIES[city] for city in TEST_CITIES}
        queue = Queue()
        result_queue = Queue()
        data_fetch_process = DataFetchingTask(cities=CITIES_FOR_TEST, queue=queue)
        data_calculation_process = DataCalculationTask(
            start_day='2022-05-26',
            finish_day='2022-05-29',
            queue=queue,
            result_queue=result_queue
        )

        data_fetch_process.start()
        data_calculation_process.start()
        data_fetch_process.join()
        data_calculation_process.join()

        results_of_calculations = result_queue.get()
        lock = Lock()
        out_file_name = 'test.json'
        capture = io.StringIO()
        sys.stdout = capture

        data_aggregation_thread = DataAggregationTask(
            lock=lock,
            file_name=out_file_name,
            results_of_calculations=results_of_calculations
        )
        data_analyzing_thread = DataAnalyzingTask(
            lock=lock,
            file_name=out_file_name
        )

        data_aggregation_thread.start()
        data_analyzing_thread.start()

        self.assertTrue(find_file(out_file_name).endswith(out_file_name))
        self.assertEqual(capture.getvalue(), 'Лучший город для отдыха это: Каир\n')
        os.remove(out_file_name)


if __name__ == "__main__":
    unittest.main()
