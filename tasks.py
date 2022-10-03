import concurrent
from datetime import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue
from statistics import mean
from threading import Lock, Thread
from typing import Optional

from api_client import YandexWeatherAPI
from utils import FIELDS_EN_TO_RUS, find_file, get_bad_conditions_from_file


logger = logging.getLogger(__name__)

class DataFetchingTask(Process):
    """Data fetching process."""
    def __init__(self, cities: dict[str, str], queue: Queue) -> None:
        """
        :param cities: dictionary with cities-keys and urls-values
        :param queue: queue for sending results of fetching to data calculation process
        """
        super().__init__()
        self._api = YandexWeatherAPI()
        self._cities = cities
        self._queue = queue

    def _get_data_by_city(self, city: str) -> dict:
        try:
            data = self._api.get_forecasting(city)
            if data:
                data['city_name'] = city
                return data
        except Exception:
            logger.exception('Something goes wrong in _get_data_by_city method')

    def run(self):
        logger.info('Запущен процесс получения данных.')
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(self._get_data_by_city, city) for city in self._cities]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        self._queue.put(result)
                        logger.info(
                            'Got data for %s.' % result["city_name"]
                        )
                except Exception:
                    logger.exception('Something goes wrong in DataFetchingTask')
            self._queue.put(None)
            logger.info('Data fetching complete.')


class DataCalculationTask(Process):
    """
    Data calculation process.
    """

    def __init__(
            self,
            start_day: str,
            finish_day: str,
            queue: Queue,
            result_queue: Queue,
            bad_conditions : list
    ) -> None:
        """
        :param start_day: bottom day of period in format yyyy-mm-dd
        :param finish_day: top day of period in format yyyy-mm-dd
        :param queue: tasks
        :param result_queue: queue for result of calculations
        :return: None
        """
        super().__init__()
        self._queue = queue
        self._start_day = datetime.strptime(start_day, '%Y-%m-%d').date()
        self._finish_day = datetime.strptime(finish_day, '%Y-%m-%d').date()
        self._bad_conditions = bad_conditions
        self.result_queue = result_queue

    @staticmethod
    def _get_formatted_date(day: dict) -> str:
        date = datetime.strptime(day['date'], '%Y-%m-%d').date()
        return date.strftime('%d-%m')

    def _get_days_period(self, data: dict) -> list:
        return [
            day
            for day in data['forecasts']
            if self._start_day <= datetime.strptime(day['date'], '%Y-%m-%d').date() <= self._finish_day
        ]

    @staticmethod
    def _get_period_of_hours(day: dict, bottom_day_hour: int = 9, top_day_hour: int = 19) -> list:
        return [
            hour
            for hour in day['hours']
            if bottom_day_hour <= int(hour['hour']) <= top_day_hour
        ]

    def _get_filtered_dates_data(self, city_data: dict) -> Optional[dict]:
        result = {
            'city_name': city_data['city_name'],
            'dates': {
                self._get_formatted_date(day): (hours := self._get_period_of_hours(day))
                for day in self._get_days_period(city_data)
                if day['hours'] and hours
            }
        }
        if not result['dates']:
            city_name = result['city_name']
            logger.error(
                'Not found days in the given interval for %s.' % city_name
            )
            return None

        return result

    @staticmethod
    def _get_temp(hour: dict) -> Optional[int]:
        return hour['temp']

    def _get_avg_date_temp(self, date_data: list) -> Optional[int]:
        return round(mean([
            self._get_temp(hour)
            for hour in date_data
        ]))

    @staticmethod
    def _get_condition(hour: dict) -> str:
        return hour['condition']

    def _get_avg_date_cond(self, date_data: list) -> int:
        points = 0
        conditions = [
            self._get_condition(hour)
            for hour in date_data
        ]
        for condition in conditions:
            if condition not in self._bad_conditions:
                points += 1
        return points

    def _calculate_data_for_date(self, date_data: list) -> dict:
        return {
            'avg_temp': self._get_avg_date_temp(date_data),
            'cond_hours': self._get_avg_date_cond(date_data)
        }

    @staticmethod
    def _get_params_for_city(dates: dict) -> dict:
        temps = [
            date_data['avg_temp']
            for date_data in dates.values()
        ]
        mean_temp = round(mean(temps), 1)
        conditions = [
            date_data['cond_hours']
            for date_data in dates.values()
        ]
        mean_cond = round(mean(conditions), 1)
        total_score = mean_temp + mean_cond
        return {
            'AVG': {
                'avg_temp': mean_temp,
                'cond_hours': mean_cond
            },
            'total_score': total_score
        }

    def _calculate_city_data(self, data: dict) -> Optional[dict]:
        filtered_data = self._get_filtered_dates_data(data)
        if not filtered_data:
            return None
        result_of_calculations = {
            'city_name': filtered_data['city_name']
        }
        try:
            result_of_calculations['dates'] = {
                date:
                    self._calculate_data_for_date(date_data)
                for date, date_data in filtered_data['dates'].items()
            }
        except Exception:
            logger.exception('Something goes wrong in _calculate_dates_data method')
        avg_data_for_city = self._get_params_for_city(
            result_of_calculations['dates']
        )
        result_of_calculations.update(avg_data_for_city)
        return result_of_calculations


    def run(self):
        logger.info('Run process of data calculation.')
        results = []
        while (data_from_api := self._queue.get()) is not None:
            if (city_data := self._calculate_city_data(data_from_api))
                results.append(city_data)
            logger.info(
                'Results is calculated for  %s.' % data_from_api["city_name"]
            )
        if results:
            results.sort(
                key=lambda index: index['total_score'],
                reverse=True
            )
            current_score = results[0]['total_score']
            rating = 1
            for index in range(len(results)):
                total_score = results[index].pop('total_score')
                if total_score == current_score:
                    results[index]['rating'] = rating
                else:
                    rating += 1
                    results[index]['rating'] = rating
                    current_score = total_score
            logger.info('Rating calculated for cities.')
        self.result_queue.put(results)
        logger.info('Data calculations is finished.')


class DataAggregationTask(Thread):

    def __init__(
            self,
            lock: Lock,
            results_of_calculations: list,
            file_name: str = 'result.json'
    ) -> None:
        super().__init__()
        self._file_name = file_name
        self._lock = lock
        self._results_of_calculations = results_of_calculations

    def _get_renamed_dict(self, data: dict) -> dict:
        try:
            result = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    if key not in FIELDS_EN_TO_RUS:
                        result[key] = self._get_renamed_dict(data[key])
                    else:
                        result[FIELDS_EN_TO_RUS[key]] = self._get_renamed_dict(data[key])
                elif isinstance(value, str):
                    result[FIELDS_EN_TO_RUS[key]] = FIELDS_EN_TO_RUS[value]
                else:
                    result[FIELDS_EN_TO_RUS[key]] = value
            return result
        except KeyError:
            logger.exception('Something goes wrong in _get_renamed_dict method')

    def start(self):
        with self._lock:
            logger.info('Lock acquire by aggregation task.')
            with ThreadPoolExecutor(max_workers=5) as pool:
                renamed_data = list(pool.map(self._get_renamed_dict, self._results_of_calculations))
            with open(self._file_name, 'w', encoding='utf-8') as file:
                logger.info('Write date in file.')
                json.dump(renamed_data, file, ensure_ascii=False, indent=2)
                logger.info('Lock release by aggregation task.')


class DataAnalyzingTask(Thread):

    def __init__(self, lock: Lock, file_name: str = 'result.json') -> None:
        super().__init__()
        self._file_name = file_name
        self._lock = lock

    def start(self):
        with self._lock:
            logger.info('Start analysing')
            with open(self._file_name) as file:
                aggregation_result = json.load(file)
            result = [
                data['Город']
                for data in aggregation_result
                if data['Рейтинг'] == 1
            ]
            sentence_start = 'The best city' if len(result) == 1 else 'The best cities'
            result_for_print = ', '.join(result)
            logger.info(f'{sentence_start} for a vacation is - {result_for_print}')
