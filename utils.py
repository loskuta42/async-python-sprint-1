import os
from multiprocessing import Queue, Process

CITIES = {
    "MOSCOW": "https://code.s3.yandex.net/async-module/moscow-response.json",
    "PARIS": "https://code.s3.yandex.net/async-module/paris-response.json",
    "LONDON": "https://code.s3.yandex.net/async-module/london-response.json",
    "BERLIN": "https://code.s3.yandex.net/async-module/berlin-response.json",
    "BEIJING": "https://code.s3.yandex.net/async-module/beijing-response.json",
    "KAZAN": "https://code.s3.yandex.net/async-module/kazan-response.json",
    "SPETERSBURG": "https://code.s3.yandex.net/async-module/spetersburg-response.json",
    "VOLGOGRAD": "https://code.s3.yandex.net/async-module/volgograd-response.json",
    "NOVOSIBIRSK": "https://code.s3.yandex.net/async-module/novosibirsk-response.json",
    "KALININGRAD": "https://code.s3.yandex.net/async-module/kaliningrad-response.json",
    "ABUDHABI": "https://code.s3.yandex.net/async-module/abudhabi-response.json",
    "WARSZAWA": "https://code.s3.yandex.net/async-module/warszawa-response.json",
    "BUCHAREST": "https://code.s3.yandex.net/async-module/bucharest-response.json",
    "ROMA": "https://code.s3.yandex.net/async-module/roma-response.json",
    "CAIRO": "https://code.s3.yandex.net/async-module/cairo-response.json",
}

FIELDS_EN_TO_RUS = {
    'ABUDHABI': 'Абу Даби',
    'BEIJING': 'Пекин',
    'BERLIN': 'Берлин',
    'BUCHAREST': 'Бухарест',
    'CAIRO': 'Каир',
    'KALININGRAD': 'Калининград',
    'KAZAN': 'Казань',
    'LONDON': 'Лондон',
    'MOSCOW': 'Москва',
    'NOVOSIBIRSK': 'Новосибирск',
    'PARIS': 'Париж',
    'ROMA': 'Рим',
    'SPETERSBURG': 'Санкт-Петербург',
    'VOLGOGRAD': 'Волгоград',
    'WARSZAWA': 'Варшава',
    'avg_temp': 'Температура, среднее',
    'cond_hours': 'Без осадков, часов',
    'AVG': 'Среднее',
    'dates': 'День',
    'rating': 'Рейтинг'

}


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
                if line.find(word) != -1 and line.split()[0] not in bad_conditions_res:
                    bad_conditions_res.append(line.split()[0])
                # if line.find(word) != -1 and (condition := line.split()[0]) not in bad_conditions_res:
                #     bad_conditions_res.append(condition)
    return tuple(bad_conditions_res)


class Producer(Process):
    def __init__(self, queue: Queue, functions: [list]):
        Process.__init__(self)
        self._queue = queue
        self._functions = functions

    def run(self):
        for function in self._functions:
            self._queue.put(function)
            # time.sleep(1)


class Consumer(Process):
    def __init__(self, queue: Queue):
        Process.__init__(self)
        self._queue = queue
        self._temp_data = None

    def run(self):
        while True:
            if self._queue.empty():
                break
            else:
                # time.sleep(1)
                function = self._queue.get()
                print(function.__name__)
                self._temp_data = function(self._temp_data) if self._temp_data else function()


BAD_CONDITIONS = get_bad_conditions_from_file(path_to_file)

ERR_MESSAGE_TEMPLATE = "Something wrong. Please contact with mentor."

MIN_MAJOR_PYTHON_VER = 3
MIN_MINOR_PYTHON_VER = 9


def check_python_version():
    import sys

    if (
            sys.version_info.major < MIN_MAJOR_PYTHON_VER
            or sys.version_info.minor < MIN_MINOR_PYTHON_VER
    ):
        raise Exception(
            "Please use python version >= {}.{}".format(
                MIN_MAJOR_PYTHON_VER, MIN_MINOR_PYTHON_VER
            )
        )
