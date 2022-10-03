from multiprocessing import Queue
from threading import Lock

import pytest

from tasks import (DataAggregationTask,
                   DataAnalyzingTask,
                   DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, find_file, get_bad_conditions_from_file


@pytest.fixture
def cities_for_test():
    cities = ['MOSCOW', 'CAIRO', 'ROMA']
    return {city: CITIES[city] for city in cities}


@pytest.fixture
def city_for_test():
    return {'MOSCOW': CITIES['MOSCOW']}


@pytest.fixture
def bad_conditions():
    return get_bad_conditions_from_file(find_file('conditions.txt'))


@pytest.fixture
def queue():
    return Queue()


@pytest.fixture
def result_queue():
    return Queue()


@pytest.fixture
def lock():
    return Lock()


@pytest.fixture
def out_file_name():
    return 'test.json'


@pytest.fixture
def data_fetching_process_for_cities(
        cities_for_test,
        queue
):
    return DataFetchingTask(
        cities=cities_for_test,
        queue=queue
    )


@pytest.fixture
def city_data(
        city_for_test,
        queue
):
    data_fetch_process = DataFetchingTask(
        cities=city_for_test,
        queue=queue
    )
    data_fetch_process.start()
    data_fetch_process.join()
    return queue.get()


@pytest.fixture
def queue_with_cities(
        cities_for_test,
        queue
):
    data_fetch_process = DataFetchingTask(
        cities=cities_for_test,
        queue=queue
    )
    data_fetch_process.start()
    data_fetch_process.join()
    return queue


@pytest.fixture
def data_calculation_process(
        bad_conditions,
        queue_with_cities,
        result_queue
):
    return DataCalculationTask(
        start_day='2022-05-26',
        finish_day='2022-05-29',
        queue=queue_with_cities,
        result_queue=result_queue,
        bad_conditions=bad_conditions
    )


@pytest.fixture
def filtered_data(
        data_calculation_process,
        city_data
):
    return data_calculation_process._get_filtered_dates_data(city_data)


@pytest.fixture
def data_aggregation_thread(
        lock,
        out_file_name
):
    return DataAggregationTask(
        lock=lock,
        file_name=out_file_name,
        results_of_calculations=[]
    )


@pytest.fixture
def data_analyzing_thread(
        lock,
        out_file_name
):
    return DataAnalyzingTask(
        lock=lock,
        file_name=out_file_name
    )
