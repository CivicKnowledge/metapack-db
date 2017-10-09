# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

import string

def qualified_class_name(o):
    """Full name of an object, including the module"""
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + ':' + o.__class__.__name__


def base_encode(number, alphabet=string.ascii_lowercase, fill=None):
    """Encode a number in an arbitrary base, by default, base 26. """

    b = ''

    while number:
        number, i = divmod(number, len(alphabet))
        b = alphabet[i] + b


    v = b or alphabet[0]

    if fill:
        return (alphabet[0]*(fill-len(v)))+v
    else:
        return v

# From http://stackoverflow.com/a/295466
def tablenamify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.type(
    """
    import re
    import unicodedata
    value = str(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf8').strip().lower()
    value = re.sub(r'[^\w\s\-\.]', '', value)
    value = re.sub(r'[-\s]+', '_', value)
    return value