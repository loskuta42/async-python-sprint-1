import logging
from multiprocessing import Queue
from threading import Lock

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask
)
from utils import CITIES


logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    datefmt='%H:%M:%S',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """

    queue = Queue()
    result_queue = Queue()
    data_fetch_process = DataFetchingTask(cities=CITIES, queue=queue)
    data_calculation_process = DataCalculationTask(
        start_day='2022-05-26',
        finish_day='2022-05-29',
        queue=queue,
        result_queue=result_queue
    )
    try:
        data_fetch_process.start()
        data_calculation_process.start()
        data_fetch_process.join()
        data_calculation_process.join()
    except Exception as ex:
        logger.error(f'forecast_weather - Запуск процессов - {ex}')

    results_of_calculations = result_queue.get()
    lock = Lock()
    out_file_name = 'result.json'

    data_aggregation_thread = DataAggregationTask(
        lock=lock,
        file_name=out_file_name,
        results_of_calculations=results_of_calculations
    )
    data_analyzing_thread = DataAnalyzingTask(
        lock=lock,
        file_name=out_file_name
    )
    try:
        data_aggregation_thread.start()
        data_analyzing_thread.start()
    except Exception as ex:
        logger.error(f'forecast_weather - Запуск потоков с блокировкой- {ex}')
    logger.info('Завершено!')


if __name__ == "__main__":
    forecast_weather()
