import logging
from multiprocessing import Queue
from threading import Lock

from tasks import (DataAggregationTask,
                   DataAnalyzingTask,
                   DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, find_file, get_bad_conditions_from_file


logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    datefmt='%H:%M:%S',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s'
)


logger = logging.getLogger(__name__)

BAD_CONDITIONS = get_bad_conditions_from_file(find_file('conditions.txt'))


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
        result_queue=result_queue,
        bad_conditions=BAD_CONDITIONS
    )
    try:
        data_fetch_process.start()
        data_calculation_process.start()
        data_fetch_process.join()
        data_calculation_process.join()
    except Exception:
        logger.exception('forecast_weather func - Processes start')

    results_of_calculations = result_queue.get()
    if not results_of_calculations:
        logger.info('No data for the given time interval.')
    else:
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
        except Exception:
            logger.exception('forecast_weather func - Running threads with lock')
        logger.info('Success!')


if __name__ == "__main__":
    forecast_weather()
