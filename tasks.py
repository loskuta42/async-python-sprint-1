import logging
from concurrent.futures import ThreadPoolExecutor
import os
from typing import Callable
from multiprocessing import Process, Queue

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
    with open(path_to_file, encoding='utf-8') as file:
        for line in file:
            for word in bad_conditions_words:
                # TODO добавить моржовый оператор
                if line.find(word) != -1 and line.split()[0] not in bad_conditions_res:
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

    def __init__(self, data: dict, bottom_day: str, top_day: str) -> None:
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
        # self._queue = Queue()
        # self._Producer = Producer
        # self._Consumer = Consumer

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
            return [hour for hour in day['hours'] if bottom_day_hour <= int(hour['hour']) <= top_day_hour]
        except Exception as e:
            logging.error(e)

    def _get_filtered_dates_data(self, date_data: dict) -> dict:

        try:
            return {
                self._get_formatted_date(day): self._get_period_of_hours(day)
                for day in self._get_days_period(date_data)
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

    def _get_avg_date_temp(self, date_data: list) -> int:
        try:
            return round(mean([self._get_temp(hour) for hour in date_data]))
        except Exception as e:
            logging.error(f'_get_avg_date_temp: {e}')

    @staticmethod
    def _get_condition(hour: dict) -> str:
        try:
            return hour['condition']
        except KeyError as e:
            logging.error(f'_get_condition: {e}')

    def _get_avg_date_cond(self, date_data: list) -> int:
        points = 0
        conditions = [self._get_condition(hour) for hour in date_data]
        for condition in conditions:
            if condition not in self._bad_conditions:
                points += 1
        return points
        #
        # try:
        #     return round(mean([self._get_temp(hour) for hour in hours]))
        # except Exception as e:
        #     logging.error(f'_get_avg_date_temp: {e}')

    def _calculate_data_for_date(self, date_data: list) -> dict:
        return {
            'avg_temp': self._get_avg_date_temp(date_data),
            'cond_points': self._get_avg_date_cond(date_data)
        }

    @staticmethod
    def _get_params_for_city(data: dict) -> dict:
        mean_temp = round(mean([date_data['avg_temp'] for date_data in data.values()]), 1)
        mean_cond = round(mean([date_data['cond_points'] for date_data in data.values()]), 1)
        total_score = mean_temp + mean_cond
        return {
            'avg_temp_for_city': round(mean_temp, 1),
            'avg_cond_for_city': round(mean_cond, 1),
            'total_score': total_score
        }

    @staticmethod
    def _calculate_rating(data: dict):
        pass

    def _calculate_dates_data(self, filtered_data: dict) -> dict:
        try:
            result = {
                date: self._calculate_data_for_date(date_data)
                for date, date_data in filtered_data.items()
                if date_data
            }
            avg_data_for_city = self._get_params_for_city(result)
            result.update(avg_data_for_city)
            return result
        except Exception as e:
            logging.error(f'_calculate_dates_data: {e}')

    def calculate_data(self) -> dict:
        result = {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            city_to_future = {
                city: pool.submit(self._calculate_dates_data, self._get_filtered_dates_data(data))
                for city, data in self._data.items()
            }
            for city, future in city_to_future.items():
                try:
                    result[city] = future.result()
                except Exception as e:
                    logging.error('%r generated an exception in calculate_data method: %s' % (city, e))
        sorted_city_by_score = sorted(result, key=result.get('total_score'))
        current_score = result[sorted_city_by_score[0]]['total_score']
        rating = 1
        for city in sorted_city_by_score:
            if result[city]['total_score'] == current_score:
                result[city]['rating'] = rating
            else:
                rating += 1
                result[city]['rating'] = rating
                current_score = result[city]['total_score']
        return result


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


if __name__ == '__main__':
    from utils import CITIES

    data = DataFetchingTask(CITIES).get_data()
    # print(data['KALININGRAD'])
    print(DataCalculationTask(data, '2022-05-26', '2022-05-29').calculate_data())
    res = {'MOSCOW': {'26-05': {'avg_temp': 18, 'cond_points': 7}, '27-05': {'avg_temp': 13, 'cond_points': 0}, '28-05': {'avg_temp': 12, 'cond_points': 0}, '29-05': {'avg_temp': 12, 'cond_points': 1}, 'avg_temp_for_city': 13.8, 'avg_cond_for_city': 2, 'total_score': 15.8}, 'PARIS': {'26-05': {'avg_temp': 18, 'cond_points': 9}, '27-05': {'avg_temp': 17, 'cond_points': 11}, '28-05': {'avg_temp': 17, 'cond_points': 11}, 'avg_temp_for_city': 17.3, 'avg_cond_for_city': 10.3, 'total_score': 27.6}, 'LONDON': {'26-05': {'avg_temp': 17, 'cond_points': 11}, '27-05': {'avg_temp': 16, 'cond_points': 11}, '28-05': {'avg_temp': 15, 'cond_points': 11}, 'avg_temp_for_city': 16, 'avg_cond_for_city': 11, 'total_score': 27}, 'BERLIN': {'26-05': {'avg_temp': 19, 'cond_points': 9}, '27-05': {'avg_temp': 16, 'cond_points': 6}, '28-05': {'avg_temp': 14, 'cond_points': 0}, 'avg_temp_for_city': 16.3, 'avg_cond_for_city': 5, 'total_score': 21.3}, 'BEIJING': {'26-05': {'avg_temp': 32, 'cond_points': 11}, '27-05': {'avg_temp': 33, 'cond_points': 11}, '28-05': {'avg_temp': 34, 'cond_points': 11}, '29-05': {'avg_temp': 28, 'cond_points': 6}, 'avg_temp_for_city': 31.8, 'avg_cond_for_city': 9.8, 'total_score': 41.6}, 'KAZAN': {'26-05': {'avg_temp': 13, 'cond_points': 6}, '27-05': {'avg_temp': 14, 'cond_points': 0}, '28-05': {'avg_temp': 15, 'cond_points': 3}, '29-05': {'avg_temp': 14, 'cond_points': 1}, 'avg_temp_for_city': 14, 'avg_cond_for_city': 2.5, 'total_score': 16.5}, 'SPETERSBURG': {'26-05': {'avg_temp': 12, 'cond_points': 3}, '27-05': {'avg_temp': 12, 'cond_points': 0}, '28-05': {'avg_temp': 12, 'cond_points': 0}, '29-05': {'avg_temp': 12, 'cond_points': 1}, 'avg_temp_for_city': 12, 'avg_cond_for_city': 1, 'total_score': 13}, 'VOLGOGRAD': {'26-05': {'avg_temp': 22, 'cond_points': 11}, '27-05': {'avg_temp': 22, 'cond_points': 11}, '28-05': {'avg_temp': 26, 'cond_points': 11}, '29-05': {'avg_temp': 25, 'cond_points': 1}, 'avg_temp_for_city': 23.8, 'avg_cond_for_city': 8.5, 'total_score': 32.3}, 'NOVOSIBIRSK': {'26-05': {'avg_temp': 25, 'cond_points': 11}, '27-05': {'avg_temp': 22, 'cond_points': 11}, '28-05': {'avg_temp': 23, 'cond_points': 11}, '29-05': {'avg_temp': 22, 'cond_points': 5}, 'avg_temp_for_city': 23, 'avg_cond_for_city': 9.5, 'total_score': 32.5}, 'KALININGRAD': {'26-05': {'avg_temp': 15, 'cond_points': 9}, '27-05': {'avg_temp': 13, 'cond_points': 2}, '28-05': {'avg_temp': 12, 'cond_points': 1}, 'avg_temp_for_city': 13.3, 'avg_cond_for_city': 4, 'total_score': 17.3}, 'ABUDHABI': {'26-05': {'avg_temp': 35, 'cond_points': 11}, '27-05': {'avg_temp': 34, 'cond_points': 11}, '28-05': {'avg_temp': 34, 'cond_points': 11}, '29-05': {'avg_temp': 34, 'cond_points': 2}, 'avg_temp_for_city': 34.2, 'avg_cond_for_city': 8.8, 'total_score': 43.0}, 'WARSZAWA': {'26-05': {'avg_temp': 20, 'cond_points': 11}, '27-05': {'avg_temp': 14, 'cond_points': 3}, '28-05': {'avg_temp': 13, 'cond_points': 0}, 'avg_temp_for_city': 15.7, 'avg_cond_for_city': 4.7, 'total_score': 20.4}, 'BUCHAREST': {'26-05': {'avg_temp': 27, 'cond_points': 11}, '27-05': {'avg_temp': 26, 'cond_points': 11}, '28-05': {'avg_temp': 28, 'cond_points': 11}, '29-05': {'avg_temp': 18, 'cond_points': 1}, 'avg_temp_for_city': 24.8, 'avg_cond_for_city': 8.5, 'total_score': 33.3}, 'ROMA': {'26-05': {'avg_temp': 29, 'cond_points': 11}, '27-05': {'avg_temp': 29, 'cond_points': 11}, '28-05': {'avg_temp': 26, 'cond_points': 11}, 'avg_temp_for_city': 28, 'avg_cond_for_city': 11, 'total_score': 39}, 'CAIRO': {'26-05': {'avg_temp': 33, 'cond_points': 11}, '27-05': {'avg_temp': 33, 'cond_points': 11}, '28-05': {'avg_temp': 34, 'cond_points': 11}, 'avg_temp_for_city': 33.3, 'avg_cond_for_city': 11, 'total_score': 44.3}}
    print(sorted(res, key=res.get('total_score')))
    # print(obj._get_filtered_dates_data(data['KALININGRAD']))
    # temp = obj._calculate_dates_data(temp)
    # time_s = time.time()
    # from utils import CITIES
    #
    # DataFetchingTask(CITIES).get_data()
    # import os
