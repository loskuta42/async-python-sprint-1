import os
from multiprocessing import Queue
from threading import Lock

from tasks import (DataAggregationTask,
                   DataAnalyzingTask,
                   DataCalculationTask,
                   DataFetchingTask)
from utils import find_file, get_bad_conditions_from_file


def test_find_file():
    file_name = 'tasks.py'
    assert find_file(file_name).endswith(file_name)


def test_get_bad_conditions():
    test_file_name = 'test.txt'
    with open(test_file_name, 'w') as file:
        file.write('snow — снег.\n')
        file.write('cloudy — облачно с прояснениями.\n')
        file.write('light-rain — небольшой дождь.\n')
        file.write('overcast — пасмурно.\n')
    bad_cond = get_bad_conditions_from_file(test_file_name)
    assert 'overcast' not in bad_cond
    assert 'snow' in bad_cond
    assert 'light-rain' in bad_cond
    os.remove(test_file_name)


def test_data_fetching(
        data_fetching_process_for_cities,
        cities_for_test,
        queue
):
    data_fetching_process_for_cities.start()
    data_fetching_process_for_cities.join()
    data_for_cities = []
    while (data := queue.get()) is not None:
        data_for_cities.append(data)
    assert len(data_for_cities) == 3
    for city_data in data_for_cities:
        assert city_data['city_name'] in cities_for_test
        assert 'forecasts' in city_data


def test_get_days_period_method(
        city_data,
        data_calculation_process
):
    days_data = data_calculation_process._get_days_period(city_data)
    assert days_data
    assert len(days_data) == 4


def test_get_period_of_hours_method(
        data_calculation_process,
        city_data
):
    hours_data = data_calculation_process._get_period_of_hours(city_data['forecasts'][0])
    assert hours_data
    assert len(hours_data) == 11


def test_get_formatted_date_method(
        data_calculation_process,
):
    assert data_calculation_process._get_formatted_date({'date': '2022-05-17'}) == '17-05'


def test_get_filtered_dates_data_method(
        city_data,
        data_calculation_process
):
    filtered_dates_data = data_calculation_process._get_filtered_dates_data(city_data)
    assert filtered_dates_data
    assert len(filtered_dates_data['dates']) == 4
    assert '26-05' in filtered_dates_data['dates']


def test_correctness_of_filtered_data(
        filtered_data
):
    date = filtered_data['dates']['26-05']
    assert date
    hour = date[0]
    assert hour


def test_get_temp_method(
        data_calculation_process
):
    assert data_calculation_process._get_temp({'temp': 9}) == 9


def test_get_avg_date_temp_method(
        data_calculation_process
):
    test_list = [{'temp': temp} for temp in range(1, 4)]
    assert data_calculation_process._get_avg_date_temp(test_list) == 2


def test_get_condition_method(
        data_calculation_process
):
    assert data_calculation_process._get_condition({'condition': 'snow'}) == 'snow'


def test_get_avg_date_cond_method(
        data_calculation_process
):
    test_list = [{'condition': cond} for cond in ['snow', 'overcast', 'cloudy']]
    assert data_calculation_process._get_avg_date_cond(test_list) == 2


def test_calculate_data_for_date_method(
        data_calculation_process,
        filtered_data
):
    date = filtered_data['dates']['26-05']
    date_data_w_avg = data_calculation_process._calculate_data_for_date(date)
    assert date_data_w_avg.get('avg_temp')
    assert date_data_w_avg.get('cond_hours')


def test_get_params_for_city_method(
        data_calculation_process,
):
    dates_with_avg = {
        '26-05': {'avg_temp': 14, 'cond_hours': 3},
        '27-05': {'avg_temp': 16, 'cond_hours': 5}
    }
    data_for_city = data_calculation_process._get_params_for_city(dates_with_avg)
    assert data_for_city['AVG']['avg_temp'] == 15
    assert data_for_city['AVG']['cond_hours'] == 4
    assert data_for_city['total_score'] == 19


def test_fetching_and_calculation_processes(
        cities_for_test,
        bad_conditions
):
    queue = Queue()
    result_queue = Queue()
    data_fetching_process = DataFetchingTask(cities=cities_for_test, queue=queue)
    data_calculation_process = DataCalculationTask(
        start_day='2022-05-26',
        finish_day='2022-05-29',
        queue=queue,
        result_queue=result_queue,
        bad_conditions=bad_conditions
    )
    data_fetching_process.start()
    data_calculation_process.start()
    data_fetching_process.join()
    data_calculation_process.join()
    result_data = result_queue.get()
    assert len(result_data) == 3
    for city_data in result_data:
        assert city_data.get('dates')
        assert city_data.get('city_name')
        assert city_data.get('AVG')
        assert city_data.get('rating')


def test_get_renamed_dict(
        data_calculation_process,
        data_aggregation_thread,
):
    data_calculation_process.start()
    data_calculation_process.join()
    data_for_city = data_calculation_process.result_queue.get()[0]
    renamed_data = data_aggregation_thread._get_renamed_dict(data_for_city)
    assert 'Город' in renamed_data
    assert 'День' in renamed_data
    assert '26-05' in renamed_data['День']
    assert 'Температура, среднее' in renamed_data['День']['26-05']
    assert 'Без осадков, часов' in renamed_data['День']['26-05']
    assert 'Среднее' in renamed_data
    assert 'Температура, среднее' in renamed_data['Среднее']
    assert 'Без осадков, часов' in renamed_data['Среднее']
    assert 'Рейтинг' in renamed_data


def test_data_aggregation_thread(
        data_aggregation_thread
):
    data_aggregation_thread.start()
    out_file_name = data_aggregation_thread._file_name
    assert find_file(out_file_name).endswith(out_file_name)
    os.remove(out_file_name)


def test_data_aggregation_and_analyzing_threads(
        data_calculation_process
):
    data_calculation_process.start()
    data_calculation_process.join()
    results_of_calculations = data_calculation_process.result_queue.get()
    lock = Lock()
    out_file_name = 'test.json'

    data_aggregation_thread = DataAggregationTask(
        lock=lock,
        file_name=out_file_name,
        results_of_calculations=results_of_calculations
    )
    data_analyzing_thread = DataAnalyzingTask(
        lock=lock,
        file_name=out_file_name
    )

    data_aggregation_thread.start()
    data_analyzing_thread.start()
    assert True
    os.remove(out_file_name)
