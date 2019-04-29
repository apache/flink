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

import sys

if sys.version > '3':
    xrange = range

__all__ = ['DataTypes']


class DataType(object):
    """
    Base class for data types.
    """
    @classmethod
    def type_name(cls):
        return cls.__name__[:-4].lower()

    def __hash__(self):
        return hash(self.type_name())

    def __eq__(self, other):
        return self.type_name() == other.type_name()

    def __ne__(self, other):
        return self.type_name() != other.type_name()


class DataTypeSingleton(type):
    """
    Metaclass for DataType
    """

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(DataTypeSingleton, cls).__call__()
        return cls._instances[cls]


class AtomicType(DataType):
    """
    An internal type used to represent everything that is not
    null, arrays, structs, and maps.
    """


class NumericType(AtomicType):
    """
    Numeric data types.
    """


class IntegralType(NumericType):
    """
    Integral data types.
    """

    __metaclass__ = DataTypeSingleton


class FractionalType(NumericType):
    """
    Fractional data types.
    """


class StringType(AtomicType):
    """
    String data type.  SQL VARCHAR
    """

    __metaclass__ = DataTypeSingleton


class BooleanType(AtomicType):
    """
    Boolean data types. SQL BOOLEAN
    """

    __metaclass__ = DataTypeSingleton


class ByteType(IntegralType):
    """
    Byte data type. SQL TINYINT
    """


class CharType(IntegralType):
    """
    Char data type. SQL CHAR
    """


class ShortType(IntegralType):
    """
    Short data types.  SQL SMALLINT (16bits)
    """


class IntegerType(IntegralType):
    """
    Int data types. SQL INT (32bits)
    """


class LongType(IntegralType):
    """
    Long data types. SQL BIGINT (64bits)
    """


class FloatType(FractionalType):
    """
    Float data type. SQL FLOAT
    """

    __metaclass__ = DataTypeSingleton


class DoubleType(FractionalType):
    """
    Double data type. SQL DOUBLE
    """

    __metaclass__ = DataTypeSingleton


class DateType(AtomicType):
    """
    Date data type.  SQL DATE
    """

    __metaclass__ = DataTypeSingleton


class TimeType(AtomicType):
    """
    Time data type. SQL TIME
    """

    __metaclass__ = DataTypeSingleton


class TimestampType(AtomicType):
    """
    Timestamp data type.  SQL TIMESTAMP
    """

    __metaclass__ = DataTypeSingleton


class DataTypes(object):
    """
    Utils for types
    """
    STRING = StringType()
    BOOLEAN = BooleanType()
    BYTE = ByteType()
    CHAR = CharType()
    SHORT = ShortType()
    INT = IntegerType()
    LONG = LongType()
    FLOAT = FloatType()
    DOUBLE = DoubleType()
    DATE = DateType()
    TIME = TimeType()
    TIMESTAMP = TimestampType()
