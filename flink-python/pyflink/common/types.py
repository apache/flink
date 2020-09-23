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


class Row(tuple):
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

    def __new__(cls, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(cls, [kwargs[n] for n in names])
            row._fields = names
            row._from_dict = True
        else:
            # create row class or objects
            row = tuple.__new__(cls, args)
        row._row_kind = RowKind.INSERT
        return row

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

    def __contains__(self, item):
        if hasattr(self, "_fields"):
            return item in self._fields
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args):
        """
        Creates new Row object
        """
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
        return _create_row(self, args, self._row_kind)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self._fields.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

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
        if key != '_fields' and key != "_from_dict" and key != "_row_kind":
            raise Exception("Row is read-only")
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
            super(Row, self).__eq__(other)

    def __hash__(self):
        if hasattr(self, "_fields"):
            return hash(self._fields)
        else:
            return super(Row, self).__hash__()
