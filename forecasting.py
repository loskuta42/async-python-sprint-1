# import logging
# import threading
# import subprocess
from multiprocessing import Queue

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, Producer, Consumer


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    queue = Queue()
    out_file_name = 'result.json'
    data_fetch_func = DataFetchingTask(list(CITIES.keys())).get_data
    data_calculation_func = DataCalculationTask('2022-05-26', '2022-05-29').calculate_data
    data_agregation_func = DataAggregationTask(out_file_name).write_data_to_file
    data_analyzing_func = DataAnalyzingTask(out_file_name).get_best_cities
    functions = [
        data_fetch_func,
        data_calculation_func,
        data_agregation_func,
        data_analyzing_func
    ]
    process_producer = Producer(queue=queue, functions=functions)
    process_consumer = Consumer(queue=queue)
    process_producer.start()
    process_consumer.start()
    process_producer.join()
    process_consumer.join()

    # city_name = "MOSCOW"
    # ywAPI = YandexWeatherAPI()
    # resp = ywAPI.get_forecasting(city_name)
    # pass


if __name__ == "__main__":
    forecast_weather()
