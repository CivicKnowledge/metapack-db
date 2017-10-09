# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from sqlalchemy.types import TypeDecorator, TEXT
import json
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.event
from sqlalchemy.ext.mutable import Mutable

Base = declarative_base()


class JSONEncoder(json.JSONEncoder):

    """A JSON encoder that turns unknown objets into a string representation of
    the type."""

    def default(self, o):

        try:
            return o.dict
        except AttributeError:
            return str(type(o))


class JSONEncodedObj(TypeDecorator):

    "Represents an immutable structure as a json-encoded string."

    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, cls=JSONEncoder)
        else:
            value = '{}'
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)

        else:
            value = {}
        return value


class MutationObj(Mutable):

    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, dict) and not isinstance(value, MutationDict):
            return MutationDict.coerce(key, value)

        if isinstance(value, list) and not isinstance(value, MutationList):
            return MutationList.coerce(key, value)

        return value

    @classmethod
    def _listen_on_attribute(cls, attribute, coerce, parent_cls):
        key = attribute.key
        if parent_cls is not attribute.class_:
            return

        # rely on "propagate" here
        parent_cls = attribute.class_

        def load(state, *args):
            val = state.dict.get(key, None)
            if coerce:
                val = cls.coerce(key, val)
                state.dict[key] = val
            if isinstance(val, cls):
                val._parents[state.obj()] = key

        def set(target, value, oldvalue, initiator):
            if not isinstance(value, cls):
                value = cls.coerce(key, value)
            if isinstance(value, cls):
                value._parents[target.obj()] = key
            if isinstance(oldvalue, cls):
                oldvalue._parents.pop(target.obj(), None)
            return value

        def pickle(state, state_dict):
            val = state.dict.get(key, None)
            if isinstance(val, cls):
                if 'ext.mutable.values' not in state_dict:
                    state_dict['ext.mutable.values'] = []
                state_dict['ext.mutable.values'].append(val)

        def unpickle(state, state_dict):
            if 'ext.mutable.values' in state_dict:
                for val in state_dict['ext.mutable.values']:
                    val._parents[state.obj()] = key

        sqlalchemy.event.listen(parent_cls,'load',load,raw=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'refresh',load,raw=True,propagate=True)
        sqlalchemy.event.listen(attribute,'set',set,raw=True,retval=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'pickle',pickle,raw=True,propagate=True)
        sqlalchemy.event.listen(parent_cls,'unpickle',unpickle,raw=True,propagate=True)

class MutationDict(Mutable, dict):

    @classmethod
    def coerce(cls, key, value):  # @ReservedAssignment
        """Convert plain dictionaries to MutationDict."""

        if not isinstance(value, MutationDict):
            if isinstance(value, dict):
                return MutationDict(value)

            # this call will raise ValueError
            return Mutable.coerce(key, value)
        else:
            return value

    def __setitem__(self, key, value):
        """Detect dictionary set events and emit change events."""
        dict.__setitem__(self, key, value)

        self.changed()

    def __delitem__(self, key):
        """Detect dictionary del events and emit change events."""

        dict.__delitem__(self, key)
        self.changed()


class MutationList(MutationObj, list):

    @classmethod
    def coerce(cls, key, value):
        """Convert plain list to MutationList."""

        if isinstance(value, str):
            value = value.strip()
            if value[0] == '[':  # It's json encoded, probably
                try:
                    value = json.loads(value)
                except ValueError:
                    raise ValueError("Failed to parse JSON: '{}' ".format(value))
            else:
                value = value.split(',')

        if not value:
            value = []

        self = MutationList((MutationObj.coerce(key, v) for v in value))
        self._key = key
        return self

    def __setitem__(self, idx, value):
        list.__setitem__(self, idx, MutationObj.coerce(self._key, value))
        self.changed()

    def __setslice__(self, start, stop, values):
        list.__setslice__(self,start,stop,(MutationObj.coerce(    self._key,    v) for v in values))
        self.changed()

    def __delitem__(self, idx):
        list.__delitem__(self, idx)
        self.changed()

    def __delslice__(self, start, stop):
        list.__delslice__(self, start, stop)
        self.changed()

    def append(self, value):
        list.append(self, MutationObj.coerce(self._key, value))
        self.changed()

    def insert(self, idx, value):
        list.insert(self, idx, MutationObj.coerce(self._key, value))
        self.changed()

    def extend(self, values):
        list.extend(self, (MutationObj.coerce(self._key, v) for v in values))
        self.changed()

    def pop(self, *args, **kw):
        value = list.pop(self, *args, **kw)
        self.changed()
        return value

    def remove(self, value):
        list.remove(self, value)
        self.changed()


def JSONAlchemy(sqltype):
    """A type to encode/decode JSON on the fly.
    sqltype is the string type for the underlying DB column.
    You can use it like:
    Column(JSONAlchemy(Text(600)))
    """
    class _JSONEncodedObj(JSONEncodedObj):
        impl = sqltype

    return MutationObj.as_mutable(_JSONEncodedObj)

