import os


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
    'rating': 'Рейтинг',
    'city_name': 'Город'

}


def find_file(name):
    path = os.getcwd()
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


def get_bad_conditions_from_file(path_to_file):
    bad_conditions_words = [
        'drizzle',
        'light-rain',
        'rain',
        'moderate-rain',
        'heavy-rain',
        'continuous-heavy-rain',
        'showers',
        'wet-snow',
        'light-snow',
        'snow',
        'snow-showers',
        'hail',
        'thunderstorm',
        'thunderstorm-with-rain',
        'thunderstorm-with-hail'
    ]
    bad_conditions_res = []
    with open(path_to_file, encoding='utf-8') as file:
        for line in file:
            if (bad_cond := line.split()[0]) in bad_conditions_words:
                bad_conditions_res.append(bad_cond)
    return tuple(bad_conditions_res)


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
