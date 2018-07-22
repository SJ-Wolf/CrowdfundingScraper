import os
from contextlib import contextmanager


def get_files_in_directory(directory='.'):
    for dirpath, dirnames, filenames in os.walk(directory):
        for fname in filenames:
            full_filename = os.path.join(dirpath, fname)
            yield full_filename


@contextmanager
def enter_folder(folder):
    orig_dir = os.getcwd()
    try:
        os.chdir(folder)
        yield
    finally:
        os.chdir(orig_dir)
