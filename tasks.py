import concurrent
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue
from statistics import mean
from threading import Lock, Thread

from api_client import YandexWeatherAPI
from utils import BAD_CONDITIONS, FIELDS_EN_TO_RUS


logger = logging.getLogger(__name__)


class DataFetchingTask(Process):
    """Data fetching process."""
    def __init__(self, cities: dict[str, str], queue: Queue) -> None:
        """
        :param cities: dictionary with cities-keys and urls-values
        :param queue: queue for sending results of fetching to data calculation process
        """
        Process.__init__(self)
        self._api = YandexWeatherAPI
        self._cities = cities
        self._queue = queue

    def _get_data_by_city(self, city: str) -> dict:
        try:
            api_obj = self._api()
            data = api_obj.get_forecasting(city)
            data['city_name'] = city
            return data
        except Exception as e:
            logger.error(f'_get_data_by_city: {e}')
        return {}

    def run(self):
        logger.info('Запущен процесс получения данных.')
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(self._get_data_by_city, city) for city in self._cities]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    self._queue.put(result)
                    logger.info(f'Получены данные для города {result["city_name"]}.')
                except Exception as ex:
                    logger.error(f'DataFetchingTask: {ex}')
            self._queue.put(None)
            logger.info('Получение данных завершено.')


class DataCalculationTask(Process):
    """
    Data calculation process.
    """

    def __init__(self, start_day: str, finish_day: str, queue: Queue, result_queue: Queue) -> None:
        """
        :param start_day: bottom day of period in format yyyy-mm-dd
        :param finish_day: top day of period in format yyyy-mm-dd
        :param queue: tasks
        :param result_queue: queue for result of calculations
        :return: None
        """
        Process.__init__(self)
        self._queue = queue
        self._start_day = start_day
        self._finish_day = finish_day
        self._bad_conditions = BAD_CONDITIONS
        self.result_queue = result_queue

    @staticmethod
    def _get_formatted_date(day: dict) -> str:
        try:
            date_lst = day['date'].split('-')
            return '-'.join(date_lst[:0:-1])
        except Exception as ex:
            logger.error(f'_get_formatted_date: {ex}')

    def _get_days_period(self, data: dict) -> list:
        try:
            return [
                day
                for day in data['forecasts']
                if self._start_day <= day['date'] <= self._finish_day
            ]
        except Exception as ex:
            logger.error(f'_get_days_period: {ex}')

    @staticmethod
    def _get_period_of_hours(day: dict, bottom_day_hour: int = 9, top_day_hour: int = 19) -> list:
        try:
            return [
                hour
                for hour in day['hours']
                if bottom_day_hour <= int(hour['hour']) <= top_day_hour
            ]
        except Exception as ex:
            logger.error(f'_get_period_of_hours: {ex}')

    def _get_filtered_dates_data(self, city_data: dict) -> dict:

        try:
            result = {
                'city_name': city_data['city_name'],
                'dates': {
                    self._get_formatted_date(day): self._get_period_of_hours(day)
                    for day in self._get_days_period(city_data)
                }
            }
            if not result['dates']:
                city_name = result['city_name']
                logger.error(
                    f'Не найдены дни в заданном промежутке для города {city_name}.'
                )
                result['dates'] = None

            return result
        except ValueError as ex:
            logger.error(ex)
        except Exception as ex:
            logger.error(f'_get_hours: {ex}')

    @staticmethod
    def _get_temp(hour: dict) -> int:
        try:
            return hour['temp']
        except KeyError as ex:
            logger.error(f'_get_temp: {ex}')

    def _get_avg_date_temp(self, date_data: list) -> int:
        try:
            return round(mean([
                self._get_temp(hour)
                for hour in date_data
            ]))
        except Exception as ex:
            logger.error(f'_get_avg_date_temp: {ex}')

    @staticmethod
    def _get_condition(hour: dict) -> str:
        try:
            return hour['condition']
        except KeyError as ex:
            logger.error(f'_get_condition: {ex}')

    def _get_avg_date_cond(self, date_data: list) -> int:
        try:
            points = 0
            conditions = [
                self._get_condition(hour)
                for hour in date_data
            ]
            for condition in conditions:
                if condition not in self._bad_conditions:
                    points += 1
            return points
        except Exception as ex:
            logger.error(f'_get_avg_date_cond: {ex}')

    def _calculate_data_for_date(self, date_data: list) -> dict:
        return {
            'avg_temp': self._get_avg_date_temp(date_data),
            'cond_hours': self._get_avg_date_cond(date_data)
        }

    @staticmethod
    def _get_params_for_city(dates: dict) -> dict:
        try:
            temps = [
                date_data['avg_temp']
                for date_data in dates.values()
                if date_data['avg_temp']
            ]
            mean_temp = round(mean(temps), 1)
            conditions = [
                date_data['cond_hours']
                for date_data in dates.values()
                if date_data['cond_hours']
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
        except Exception as ex:
            logger.error(f'_get_params_for_city: {ex}')

    def _calculate_city_data(self, data: dict) -> dict:
        filtered_data = self._get_filtered_dates_data(data)
        if not filtered_data['dates']:
            return filtered_data
        result_of_calculations = {
            'city_name': filtered_data['city_name']
        }
        try:
            result_of_calculations['dates'] = {
                date:
                    (self._calculate_data_for_date(date_data)
                     if date_data
                     else {'avg_temp': None, 'cond_hours': None}
                     )
                for date, date_data in filtered_data['dates'].items()
            }
            avg_data_for_city = self._get_params_for_city(
                result_of_calculations['dates']
            )
            result_of_calculations.update(avg_data_for_city)
            return result_of_calculations
        except Exception as ex:
            logger.error(f'_calculate_dates_data: {ex}')

    def run(self):
        logger.info('Запущен процесс расчета данных.')
        results = []
        while (data_from_api := self._queue.get()) is not None:
            results.append(self._calculate_city_data(data_from_api))
            logger.info(
                f'Посчитаны результаты для города {data_from_api["city_name"]}.'
            )
        not_none_results = [result for result in results if result['dates']]
        none_results = [result for result in results if not result['dates']]
        if not not_none_results:
            self.result_queue.put(results)
        else:
            not_none_results.sort(
                key=lambda index: index['total_score'],
                reverse=True
            )
            current_score = not_none_results[0]['total_score']
            rating = 1
            for index in range(len(not_none_results)):
                total_score = not_none_results[index].pop('total_score')
                if total_score == current_score:
                    not_none_results[index]['rating'] = rating
                else:
                    rating += 1
                    not_none_results[index]['rating'] = rating
                    current_score = total_score
        logger.info('Для городов посчитан рейтинг.')
        self.result_queue.put(not_none_results + none_results)
        logger.info('Завершен процесс расчета данных.')


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
        except KeyError as ex:
            logger.error(f'_get_renamed_dict {ex}')

    def start(self):
        with self._lock:
            logger.info('Блокировка включена потоком аггрегации.')
            with ThreadPoolExecutor(max_workers=5) as pool:
                renamed_data = list(pool.map(self._get_renamed_dict, self._results_of_calculations))
            with open(self._file_name, 'w', encoding='utf-8') as file:
                logger.info('Запись результатов в файл.')
                json.dump(renamed_data, file, ensure_ascii=False, indent=2)
                logger.info('Блокировка потоком аггрегации.')


class DataAnalyzingTask(Thread):

    def __init__(self, lock: Lock, file_name: str = 'result.json') -> None:
        super().__init__()
        self._file_name = file_name
        self._lock = lock

    def start(self):
        with self._lock:
            logger.info('Старт анализа.')
            with open(self._file_name) as file:
                aggregation_result = json.load(file)
            result = [
                data['Город']
                for data in aggregation_result
                if data['День'] is not None and data['Рейтинг'] == 1
            ]
            if result:
                sentence_start = 'Лучший город' if len(result) == 1 else 'Лучшие города'
                result_for_print = ', '.join(result)
                print(f'{sentence_start} для отдыха это: {result_for_print}')
            else:
                print('Отсутствуют данные для запрашиваемых дат.')
