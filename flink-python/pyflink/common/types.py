################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from enum import Enum

from typing import List

__all__ = ['Row', 'RowKind']


class RowKind(Enum):
    INSERT = 0
    UPDATE_BEFORE = 1
    UPDATE_AFTER = 2
    DELETE = 3


def _create_row(fields, values, row_kind: RowKind = None):
    row = Row(*values)
    if fields is not None:
        row._fields = fields
    if row_kind is not None:
        row.set_row_kind(row_kind)
    return row


class Row(object):
    """
    A row in Table.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments,
    the fields will be sorted by names. It is not allowed to omit
    a named argument to represent the value is None or missing. This should be
    explicitly set to None in this case.

    ::

        >>> row = Row(name="Alice", age=11)
        >>> row
        Row(age=11, name='Alice')
        >>> row['name'], row['age']
        ('Alice', 11)
        >>> row.name, row.age
        ('Alice', 11)
        >>> 'name' in row
        True
        >>> 'wrong_key' in row
        False

    Row can also be used to create another Row like class, then it
    could be used to create Row objects, such as

    ::

        >>> Person = Row("name", "age")
        >>> Person
        <Row(name, age)>
        >>> 'name' in Person
        True
        >>> 'wrong_key' in Person
        False
        >>> Person("Alice", 11)
        Row(name='Alice', age=11)
    """

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            names = sorted(kwargs.keys())
            self._fields = names
            self._values = [kwargs[n] for n in names]
            self._from_dict = True
        else:
            self._values = list(args)
        self._row_kind = RowKind.INSERT

    def as_dict(self, recursive=False):
        """
        Returns as a dict.

        Example:
        ::

            >>> Row(name="Alice", age=11).as_dict() == {'name': 'Alice', 'age': 11}
            True
            >>> row = Row(key=1, value=Row(name='a', age=2))
            >>> row.as_dict() == {'key': 1, 'value': Row(age=2, name='a')}
            True
            >>> row.as_dict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
            True

        :param recursive: turns the nested Row as dict (default: False).
        """
        if not hasattr(self, "_fields"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.as_dict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return dict(zip(self._fields, (conv(o) for o in self)))
        else:
            return dict(zip(self._fields, self))

    def get_row_kind(self) -> RowKind:
        return self._row_kind

    def set_row_kind(self, row_kind: RowKind):
        self._row_kind = row_kind

    def set_field_names(self, field_names: List):
        self._fields = field_names

    def _is_retract_msg(self):
        return self._row_kind == RowKind.UPDATE_BEFORE or self._row_kind == RowKind.DELETE

    def _is_accumulate_msg(self):
        return self._row_kind == RowKind.UPDATE_AFTER or self._row_kind == RowKind.INSERT

    def __contains__(self, item):
        return item in self._values

    # let object acts like class
    def __call__(self, *args):
        """
        Creates new Row object
        """
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
        return _create_row(self._values, args, self._row_kind)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self._values[item]
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(item)
            return self._values[idx]
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __setitem__(self, key, value):
        if isinstance(key, (int, slice)):
            self._values[key] = value
            return
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(key)
            self._values[idx] = value
        except (IndexError, AttributeError):
            raise KeyError(key)
        except ValueError:
            raise ValueError(value)

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key != '_fields' and key != "_from_dict" and key != "_row_kind" and key != "_values":
            raise AttributeError(key)
        self.__dict__[key] = value

    def __reduce__(self):
        """
        Returns a tuple so Python knows how to pickle Row.
        """
        if hasattr(self, "_fields"):
            return _create_row, (self._fields, tuple(self), self._row_kind)
        else:
            return _create_row, (None, tuple(self), self._row_kind)

    def __repr__(self):
        """
        Printable representation of Row used in Python REPL.
        """
        if hasattr(self, "_fields"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self._fields, tuple(self)))
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)

    def __eq__(self, other):
        if not isinstance(other, Row):
            return False
        if hasattr(self, "_fields"):
            if not hasattr(other, "_fields"):
                return False
            if self._fields != other._fields:
                return False
        else:
            if hasattr(other, "_fields"):
                return False
        return self.__class__ == other.__class__ and \
            self._row_kind == other._row_kind and \
            self._values == other._values

    def __hash__(self):
        return tuple(self).__hash__()

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)
