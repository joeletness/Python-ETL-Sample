import os
BASE_DIR = os.path.dirname(os.path.realpath(__file__))


def get_data(path):
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    _file = open(file_path, 'r')
    data = _file.read()
    _file.close()
    return data
