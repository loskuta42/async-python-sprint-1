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
    @staticmethod
    def _get_date_by_day(day: dict) -> str:
        try:
            date_lst = day['date'].split('-')
            return '-'.join(date_lst[:0:-1])
        except Exception as e:
            logging.error(e)

    def _get_hours(self, data: dict, bottom_day_hour: int = 9, top_day_hour: int = 19):
        try:
            return {
                self._get_date_by_day(day):
                    [
                        hour
                        for hour in day['hours']
                        if bottom_day_hour <= hour['hour'] <= top_day_hour
                    ]
                for day in data['forecasts']
                if self._bottom_day <= day['date'] <= self._top_day
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

    @staticmethod
    def _get_condition(hour: dict) -> str:
        try:
            return hour['condition']
        except KeyError as e:
            logging.error(f'_get_condition: {e}')


    @staticmethod
    def _get_mean_day_temp(data: list) -> int:
        try:
            return round(mean(data))
        except Exception as e:
            logging.error(e)
    @staticmethod
    def _get_average_temps_for_period():
        pass

class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


if __name__ == '__main__':
    time_s = time.time()
    from utils import CITIES

    DataFetchingTask(CITIES).get_data()
