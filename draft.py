from multiprocessing import Process, Queue
from typing import Callable
import os

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
    with open(path_to_file, encoding='utf-8') as f:
        for line in f:
            for word in bad_conditions_words:
                if line.find(word) != -1 and line.split()[0] not in bad_conditions_res:
                    bad_conditions_res.append(line.split()[0])
    return tuple(bad_conditions_res)


print(get_bad_conditions_from_file(path_to_file))