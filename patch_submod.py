import sys
import os


def patch_submodules_path():
    path = os.path.abspath('./tc')
    if path not in sys.path:
        sys.path.append(path)


def dummy():
    pass


patch_submodules_path()

