import logging
from concurrent.futures import ThreadPoolExecutor
import time

from statistics import mean
from api_client import YandexWeatherAPI

logging.basicConfig(
    filename='application-log.log',
    filemode='w',
    datefmt="%H:%M:%S",
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s'
)


def find_file(name):
    path = os.getcwd()
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


path_to_file = find_file('conditions.txt')


def get_bad_conditions_from_file(path_to_file):
    bad_conditions_words = [
        'дождь',
        'град',
        'снег',
        'морось',
        'ливень',
        'гроза'
    ]
    bad_conditions_res = []
    with open(path_to_file) as f:
        for line in f:
            for word in bad_conditions_words:
                if line.find(word) == -1:
                    bad_conditions_res.append(line.split()[0])
    return tuple(bad_conditions_res)


BAD_CONDITIONS = get_bad_conditions_from_file(path_to_file)


class DataFetchingTask:
    def __init__(self, cities: dict) -> None:
        self._api = YandexWeatherAPI
        self._cities = cities

    def _get_data_by_city(self, city: str) -> dict:
        try:
            api_obj = self._api()
            return api_obj.get_forecasting(city)
        except Exception as e:
            logging.error(e)
        return {}

    def get_data(self) -> dict:
        """
        :return: data for each city as dict
        """
        result = {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            city_to_future = {city: pool.submit(self._get_data_by_city, city) for city in self._cities.keys()}
            for city, future in city_to_future.items():
                try:
                    result[city] = future.result()
                except Exception as e:
                    logging.error('%r generated an exception in get_data method: %s' % (city, e))
        return result


class DataCalculationTask:
    """
    Base class for data preprocessing and calculation
    """

    def __int__(self, data: dict, bottom_day: str, top_day: str) -> None:
        """
        :param data: data for calculations as dict
        :param bottom_day: bottom day of period in format yyyy-mm-dd
        :param top_day: top day of period in format yyyy-mm-dd
        :return: None
        """
        self._data = data
        self._bottom_day = bottom_day
        self._top_day = top_day
        self._bad_conditions = BAD_CONDITIONS

    @staticmethod
    def _get_formatted_date(day: dict) -> str:
        try:
            date_lst = day['date'].split('-')
            return '-'.join(date_lst[:0:-1])
        except Exception as e:
            logging.error(e)

    def _get_days_period(self, data: dict) -> list:
        try:
            return [day for day in data['forecasts'] if self._bottom_day <= day['date'] <= self._top_day]
        except Exception as e:
            logging.error(e)

    @staticmethod
    def _get_period_of_hours(day: dict, bottom_day_hour: int = 9, top_day_hour: int = 19) -> list:
        try:
            return [hour for hour in day['hours'] if bottom_day_hour <= hour['hour'] <= top_day_hour]
        except Exception as e:
            logging.error(e)

    def _get_filtered_date_data(self, data: dict) -> dict:

        try:
            return {
                self._get_formatted_date(day): self._get_period_of_hours(day)
                # TODO добавить потоки
                for day in self._get_days_period(data)
            }
        # {'15-05':[[hour], [hour]]}
        except Exception as e:
            logging.error(f'_get_hours: {e}')

    @staticmethod
    def _get_temp(hour: dict) -> int:
        try:
            return hour['temp']
        except KeyError as e:
            logging.error(f'_get_temp: {e}')

    def _get_avg_date_temp(self, hours: list) -> int:
        try:
            return round(mean([self._get_temp(hour) for hour in hours]))
        except Exception as e:
            logging.error(f'_get_avg_date_temp: {e}')

    @staticmethod
    def _get_condition(hour: dict) -> str:
        try:
            return hour['condition']
        except KeyError as e:
            logging.error(f'_get_condition: {e}')

    def _get_avg_date_cond(self, hours: list) -> int:
        points = 0
        conditions = [self._get_condition(hour) for hour in hours]
        for condition in conditions:
            if condition not in self._bad_conditions:
                points += 1
        return points
        #
        # try:
        #     return round(mean([self._get_temp(hour) for hour in hours]))
        # except Exception as e:
        #     logging.error(f'_get_avg_date_temp: {e}')


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


if __name__ == '__main__':
    # time_s = time.time()
    # from utils import CITIES
    #
    # DataFetchingTask(CITIES).get_data()
    import os
