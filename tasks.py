import logging
from concurrent.futures import ThreadPoolExecutor
import time
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
        result = {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            city_to_future = {city: pool.submit(self._get_data_by_city, city) for city in self._cities.keys()}
            for city, future in city_to_future.items():
                try:
                    result[city] = future.result()
                except Exception as e:
                    logging.error('%r generated an exception: %s' % (city, e))
        return result





class DataCalculationTask:
    def __int__(self, data):
        self._data = data



class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


if __name__ == '__main__':
    time_s = time.time()
    from utils import CITIES
    DataFetchingTask(CITIES).get_data()
    print(time.time() - time_s)