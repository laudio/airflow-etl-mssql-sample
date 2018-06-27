''' Utility functions. '''

import os

def is_string(obj):
    ''' Check if the object is a string. '''
    try:
        basestring
    except NameError:
        basestring = str

    return isinstance(obj, basestring)


def is_iterable(obj):
    ''' Check if the object is iterable. '''
    has_iter = hasattr(obj, '__iter__')
    has_get_item = hasattr(obj, '__getitem__')

    return has_iter or has_get_item

def set_env(env_dict):
    for env_key, value in env_dict.items():
        os.environ[env_key] = value
