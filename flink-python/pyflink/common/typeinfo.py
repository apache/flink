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
import calendar
import datetime
import time
from abc import ABC
from enum import Enum
from typing import List, Union

from py4j.java_gateway import JavaClass, JavaObject
from pyflink.java_gateway import get_gateway


class TypeInformation(object):
    """
    TypeInformation is the core class of Flink's type system. FLink requires a type information
    for all types that are used as input or return type of a user function. This type information
    class acts as the tool to generate serializers and comparators, and to perform semantic checks
    such as whether the fields that are used as join/grouping keys actually exist.

    The type information also bridges between the programming languages object model and a logical
    flat schema. It maps fields from the types to columns (fields) in a flat schema. Not all fields
    from a type are mapped to a separate fields in the flat schema and often, entire types are
    mapped to one field.. It is important to notice that the schema must hold for all instances of a
    type. For that reason, elements in lists and arrays are not assigned to individual fields, but
    the lists and arrays are considered to be one field in total, to account for different lengths
    in the arrays.
        a) Basic types are indivisible and are considered as a single field.
        b) Arrays and collections are one field.
        c) Tuples represents as many fields as the class has fields.
    To represent this properly, each type has an arity (the number of fields it contains directly),
    and a total number of fields (number of fields in the entire schema of this type, including
    nested types).
    """

    def __init__(self):
        self._j_typeinfo = None

    def get_java_type_info(self) -> JavaObject:
        return self._j_typeinfo

    def need_conversion(self):
        """
        Does this type need to conversion between Python object and internal Wrapper object.
        """
        return False

    def to_internal_type(self, obj):
        """
        Converts a Python object into an internal object.
        """
        return obj

    def from_internal_type(self, obj):
        """
         Converts an internal object into a native Python object.
         """
        return obj


class BasicTypes(Enum):
    STRING = "String"
    BYTE = "Byte"
    BOOLEAN = "Boolean"
    SHORT = "Short"
    INT = "Integer"
    LONG = "Long"
    FLOAT = "Float"
    DOUBLE = "Double"
    CHAR = "Char"
    BIG_INT = "BigInteger"
    BIG_DEC = "BigDecimal"


class BasicTypeInformation(TypeInformation, ABC):
    """
    Type information for primitive types (int, long, double, byte, ...), String, BigInteger,
    and BigDecimal.
    """

    def __init__(self, basic_type: BasicTypes):
        self._basic_type = basic_type
        super(BasicTypeInformation, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if self._basic_type == BasicTypes.STRING:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO
        elif self._basic_type == BasicTypes.BYTE:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO
        elif self._basic_type == BasicTypes.BOOLEAN:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO
        elif self._basic_type == BasicTypes.SHORT:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO
        elif self._basic_type == BasicTypes.INT:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
        elif self._basic_type == BasicTypes.LONG:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO
        elif self._basic_type == BasicTypes.FLOAT:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO
        elif self._basic_type == BasicTypes.DOUBLE:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO
        elif self._basic_type == BasicTypes.CHAR:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.CHAR_TYPE_INFO
        elif self._basic_type == BasicTypes.BIG_INT:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_INT_TYPE_INFO
        elif self._basic_type == BasicTypes.BIG_DEC:
            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO
        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if isinstance(o, BasicTypeInformation):
            return self._basic_type == o._basic_type
        return False

    def __repr__(self):
        return self._basic_type.value

    @staticmethod
    def STRING_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.STRING)

    @staticmethod
    def BOOLEAN_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.BOOLEAN)

    @staticmethod
    def BYTE_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.BYTE)

    @staticmethod
    def SHORT_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.SHORT)

    @staticmethod
    def INT_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.INT)

    @staticmethod
    def LONG_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.LONG)

    @staticmethod
    def FLOAT_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.FLOAT)

    @staticmethod
    def DOUBLE_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.DOUBLE)

    @staticmethod
    def CHAR_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.CHAR)

    @staticmethod
    def BIG_INT_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.BIG_INT)

    @staticmethod
    def BIG_DEC_TYPE_INFO():
        return BasicTypeInformation(BasicTypes.BIG_DEC)


class SqlTimeTypeInfo(TypeInformation, ABC):
    """
    SqlTimeTypeInfo enables users to get Sql Time TypeInfo.
    """

    @staticmethod
    def DATE():
        return DateTypeInformation()

    @staticmethod
    def TIME():
        return TimeTypeInformation()

    @staticmethod
    def TIMESTAMP():
        return TimestampTypeInformation()


class PrimitiveArrayTypeInformation(TypeInformation, ABC):
    """
    A TypeInformation for arrays of primitive types (int, long, double, ...).
    Supports the creation of dedicated efficient serializers for these types.
    """

    def __init__(self, element_type: TypeInformation):
        self._element_type = element_type
        super(PrimitiveArrayTypeInformation, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        JPrimitiveArrayTypeInfo = get_gateway().jvm.org.apache.flink.api.common.typeinfo \
            .PrimitiveArrayTypeInfo
        if self._element_type == Types.BOOLEAN():
            return JPrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.BYTE():
            return JPrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.SHORT():
            return JPrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.INT():
            return JPrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.LONG():
            return JPrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.FLOAT():
            return JPrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.DOUBLE():
            return JPrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
        elif self._element_type == Types.CHAR():
            return JPrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO
        else:
            raise TypeError("Invalid element type for a primitive array.")

    def __eq__(self, o) -> bool:
        if isinstance(o, PrimitiveArrayTypeInformation):
            return self._element_type == o._element_type
        return False

    def __repr__(self) -> str:
        return "PrimitiveArrayTypeInformation<%s>" % self._element_type


def is_primitive_array_type_info(type_info: TypeInformation):
    return isinstance(type_info, PrimitiveArrayTypeInformation)


class BasicArrayTypeInformation(TypeInformation, ABC):
    """
    A TypeInformation for arrays of boxed primitive types (Integer, Long, Double, ...).
    Supports the creation of dedicated efficient serializers for these types.
    """

    def __init__(self, element_type: TypeInformation):
        self._element_type = element_type
        super(BasicArrayTypeInformation, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        JBasicArrayTypeInfo = get_gateway().jvm.org.apache.flink.api.common.typeinfo \
            .BasicArrayTypeInfo
        if self._element_type == Types.BOOLEAN():
            return JBasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
        elif self._element_type == Types.BYTE():
            return JBasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO
        elif self._element_type == Types.SHORT():
            return JBasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
        elif self._element_type == Types.INT():
            return JBasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
        elif self._element_type == Types.LONG():
            return JBasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
        elif self._element_type == Types.FLOAT():
            return JBasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
        elif self._element_type == Types.DOUBLE():
            return JBasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
        elif self._element_type == Types.CHAR():
            return JBasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
        elif self._element_type == Types.STRING():
            return JBasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        else:
            raise TypeError("Invalid element type for a primitive array.")

    def __eq__(self, o) -> bool:
        if isinstance(o, BasicArrayTypeInformation):
            return self._element_type == o._element_type
        return False

    def __repr__(self):
        return "BasicArrayTypeInformation<%s>" % self._element_type


def is_basic_array_type_info(type_info: TypeInformation):
    return isinstance(type_info, BasicArrayTypeInformation)


class PickledBytesTypeInfo(TypeInformation, ABC):
    """
    A PickledBytesTypeInfo indicates the data is a primitive byte array generated by pickle
    serializer.
    """

    @staticmethod
    def PICKLED_BYTE_ARRAY_TYPE_INFO():
        return PickledBytesTypeInfo()

    def get_java_type_info(self) -> JavaObject:
        self._j_typeinfo = get_gateway().jvm.org.apache.flink.streaming.api.typeinfo.python\
            .PickledByteArrayTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, PickledBytesTypeInfo)

    def __repr__(self):
        return "PickledByteArrayTypeInformation"


class RowTypeInfo(TypeInformation):
    """
    TypeInformation for Row.
    """

    def __init__(self, types: List[TypeInformation], field_names: List[str] = None):
        self.types = types
        self.field_names = field_names
        self._need_conversion = [f.need_conversion() if isinstance(f, TypeInformation) else None
                                 for f in self.types]
        self._need_serialize_any_field = any(self._need_conversion)
        super(RowTypeInfo, self).__init__()

    def get_field_names(self) -> List[str]:
        if not self.field_names:
            j_field_names = self.get_java_type_info().getFieldNames()
            self.field_names = [name for name in j_field_names]
        return self.field_names

    def get_field_index(self, field_name: str) -> int:
        if self.field_names:
            return self.field_names.index(field_name)
        return -1

    def get_field_types(self) -> List[TypeInformation]:
        return self.types

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            j_types_array = get_gateway()\
                .new_array(get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation,
                           len(self.types))
            for i in range(len(self.types)):
                wrapper_typeinfo = self.types[i]
                if isinstance(wrapper_typeinfo, TypeInformation):
                    j_types_array[i] = wrapper_typeinfo.get_java_type_info()

            if self.field_names is None:
                self._j_typeinfo = get_gateway().jvm\
                    .org.apache.flink.api.java.typeutils.RowTypeInfo(j_types_array)
            else:
                j_names_array = get_gateway().new_array(get_gateway().jvm.java.lang.String,
                                                        len(self.field_names))
                for i in range(len(self.field_names)):
                    j_names_array[i] = self.field_names[i]
                self._j_typeinfo = get_gateway().jvm\
                    .org.apache.flink.api.java.typeutils.RowTypeInfo(j_types_array, j_names_array)
        return self._j_typeinfo

    def __eq__(self, other) -> bool:
        if isinstance(other, RowTypeInfo):
            return self.types == other.types
        return False

    def __str__(self) -> str:
        if self.field_names:
            return "RowTypeInfo(%s)" % ', '.join([field_name + ': ' + str(field_type)
                                                  for field_name, field_type in
                                                  zip(self.get_field_names(),
                                                      self.get_field_types())])
        else:
            return "RowTypeInfo(%s)" % ', '.join([str(field_type) for field_type in self.types])

    def need_conversion(self):
        return True

    def to_internal_type(self, obj):
        if obj is None:
            return

        if self._need_serialize_any_field:
            # Only calling to_internal_type function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(f.to_internal_type(obj.get(n)) if c else obj.get(n)
                             for n, f, c in zip(self.field_names, self.types,
                                                self._need_conversion))
            elif isinstance(obj, (tuple, list)):
                return tuple(f.to_internal_type(v) if c else v
                             for f, v, c in zip(self.types, obj, self._need_conversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(f.to_internal_type(d.get(n)) if c else d.get(n)
                             for n, f, c in zip(self._j_typeinfo.getFieldNames(), self.types,
                                                self._need_conversion))
            else:
                raise ValueError("Unexpected tuple %r with RowTypeInfo" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.field_names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.field_names)
            else:
                raise ValueError("Unexpected tuple %r with RowTypeInfo" % obj)

    def from_internal_type(self, obj):
        if obj is None:
            return
        if isinstance(obj, (tuple, list)):
            # it's already converted by pickler
            return obj
        if self._need_serialize_any_field:
            # Only calling from_internal_type function for fields that need conversion
            values = [f.from_internal_type(v) if c else v
                      for f, v, c in zip(self.types, obj, self._need_conversion)]
        else:
            values = obj
        return tuple(values)


class TupleTypeInfo(TypeInformation):
    """
    TypeInformation for Tuple.
    """

    def __init__(self, types: List[TypeInformation]):
        self.types = types
        super(TupleTypeInfo, self).__init__()

    def get_field_types(self) -> List[TypeInformation]:
        return self.types

    def get_java_type_info(self) -> JavaObject:
        j_types_array = get_gateway().new_array(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation, len(self.types))

        for i in range(len(self.types)):
            field_type = self.types[i]
            if isinstance(field_type, TypeInformation):
                j_types_array[i] = field_type.get_java_type_info()

        self._j_typeinfo = get_gateway().jvm \
            .org.apache.flink.api.java.typeutils.TupleTypeInfo(j_types_array)
        return self._j_typeinfo

    def __eq__(self, other) -> bool:
        if isinstance(other, TupleTypeInfo):
            return self.types == other.types
        return False

    def __str__(self) -> str:
        return "TupleTypeInfo(%s)" % ', '.join([str(field_type) for field_type in self.types])


class DateTypeInformation(TypeInformation):
    """
    TypeInformation for Date.
    """

    def __init__(self):
        super(DateTypeInformation, self).__init__()

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def need_conversion(self):
        return True

    def to_internal_type(self, d):
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def from_internal_type(self, v):
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)

    def get_java_type_info(self) -> JavaObject:
        self._j_typeinfo = get_gateway().jvm\
            .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.DATE
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DateTypeInformation)

    def __repr__(self):
        return "DateTypeInformation"


class TimeTypeInformation(TypeInformation):
    """
    TypeInformation for Time.
    """

    EPOCH_ORDINAL = calendar.timegm(time.localtime(0)) * 10 ** 6

    def need_conversion(self):
        return True

    def to_internal_type(self, t):
        if t is not None:
            if t.tzinfo is not None:
                offset = t.utcoffset()
                offset = offset if offset else datetime.timedelta()
                offset_microseconds =\
                    (offset.days * 86400 + offset.seconds) * 10 ** 6 + offset.microseconds
            else:
                offset_microseconds = self.EPOCH_ORDINAL
            minutes = t.hour * 60 + t.minute
            seconds = minutes * 60 + t.second
            return seconds * 10 ** 6 + t.microsecond - offset_microseconds

    def from_internal_type(self, t):
        if t is not None:
            seconds, microseconds = divmod(t + self.EPOCH_ORDINAL, 10 ** 6)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return datetime.time(hours, minutes, seconds, microseconds)

    def get_java_type_info(self) -> JavaObject:
        self._j_typeinfo = get_gateway().jvm\
            .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIME
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimeTypeInformation)

    def __repr__(self) -> str:
        return "TimeTypeInformation"


class TimestampTypeInformation(TypeInformation):
    """
    TypeInformation for Timestamp.
    """

    def need_conversion(self):
        return True

    def to_internal_type(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 10 ** 6 + dt.microsecond

    def from_internal_type(self, ts):
        if ts is not None:
            return datetime.datetime.fromtimestamp(ts // 10 ** 6).replace(microsecond=ts % 10 ** 6)

    def get_java_type_info(self) -> JavaObject:
        self._j_typeinfo = get_gateway().jvm\
            .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimestampTypeInformation)

    def __repr__(self):
        return "TimestampTypeInformation"


class Types(object):
    """
    This class gives access to the type information of the most common types for which Flink has
    built-in serializers and comparators.
    """

    STRING = BasicTypeInformation.STRING_TYPE_INFO
    BYTE = BasicTypeInformation.BYTE_TYPE_INFO
    BOOLEAN = BasicTypeInformation.BOOLEAN_TYPE_INFO
    SHORT = BasicTypeInformation.SHORT_TYPE_INFO
    INT = BasicTypeInformation.INT_TYPE_INFO
    LONG = BasicTypeInformation.LONG_TYPE_INFO
    FLOAT = BasicTypeInformation.FLOAT_TYPE_INFO
    DOUBLE = BasicTypeInformation.DOUBLE_TYPE_INFO
    CHAR = BasicTypeInformation.CHAR_TYPE_INFO
    BIG_INT = BasicTypeInformation.BIG_INT_TYPE_INFO
    BIG_DEC = BasicTypeInformation.BIG_DEC_TYPE_INFO

    SQL_DATE = SqlTimeTypeInfo.DATE
    SQL_TIME = SqlTimeTypeInfo.TIME
    SQL_TIMESTAMP = SqlTimeTypeInfo.TIMESTAMP

    PICKLED_BYTE_ARRAY = PickledBytesTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO

    @staticmethod
    def ROW(types: List[TypeInformation]):
        """
        Returns type information for Row with fields of the given types. A row itself must not be
        null.

        :param types: the types of the row fields, e.g., Types.String(), Types.INT()
        """
        return RowTypeInfo(types)

    @staticmethod
    def ROW_NAMED(names: List[str], types: List[TypeInformation]):
        """
        Returns type information for Row with fields of the given types and with given names. A row
        must not be null.

        :param names: array of field names.
        :param types: array of field types.
        """
        return RowTypeInfo(types, names)

    @staticmethod
    def TUPLE(types: List[TypeInformation]):
        """
        Returns type information for Tuple with fields of the given types. A Tuple itself must not
        be null.

        :param types: array of field types.
        """
        return TupleTypeInfo(types)

    @staticmethod
    def PRIMITIVE_ARRAY(element_type: TypeInformation):
        """
        Returns type information for arrays of primitive type (such as byte[]). The array must not
        be null.

        :param element_type element type of the array (e.g. Types.BOOLEAN(), Types.INT(),
        Types.DOUBLE())
        """
        return PrimitiveArrayTypeInformation(element_type)

    @staticmethod
    def BASIC_ARRAY(element_type: TypeInformation) -> TypeInformation:
        """
        Returns type information for arrays of boxed primitive type (such as Integer[]).

        :param element_type element type of the array (e.g. Types.BOOLEAN(), Types.INT(),
        Types.DOUBLE())
        """

        return BasicArrayTypeInformation(element_type)


def _from_java_type(j_type_info: JavaObject) -> TypeInformation:
    gateway = get_gateway()
    JBasicTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo

    if _is_instance_of(j_type_info, JBasicTypeInfo.STRING_TYPE_INFO):
        return Types.STRING()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.BOOLEAN_TYPE_INFO):
        return Types.BOOLEAN()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.BYTE_TYPE_INFO):
        return Types.BYTE()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.SHORT_TYPE_INFO):
        return Types.SHORT()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.INT_TYPE_INFO):
        return Types.INT()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.LONG_TYPE_INFO):
        return Types.LONG()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.FLOAT_TYPE_INFO):
        return Types.FLOAT()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.DOUBLE_TYPE_INFO):
        return Types.DOUBLE()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.CHAR_TYPE_INFO):
        return Types.CHAR()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.BIG_INT_TYPE_INFO):
        return Types.BIG_INT()
    elif _is_instance_of(j_type_info, JBasicTypeInfo.BIG_DEC_TYPE_INFO):
        return Types.BIG_DEC()

    JSqlTimeTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
    if _is_instance_of(j_type_info, JSqlTimeTypeInfo.DATE):
        return Types.SQL_DATE()
    elif _is_instance_of(j_type_info, JSqlTimeTypeInfo.TIME):
        return Types.SQL_TIME()
    elif _is_instance_of(j_type_info, JSqlTimeTypeInfo.TIMESTAMP):
        return Types.SQL_TIMESTAMP()

    JPrimitiveArrayTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo \
        .PrimitiveArrayTypeInfo

    if _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.BOOLEAN())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.BYTE())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.SHORT())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.INT())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.LONG())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.FLOAT())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.DOUBLE())
    elif _is_instance_of(j_type_info, JPrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO):
        return Types.PRIMITIVE_ARRAY(Types.CHAR())

    JBasicArrayTypeInfo = gateway.jvm.org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo

    if _is_instance_of(j_type_info, JBasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.BOOLEAN())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.BYTE())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.SHORT())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.INT_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.INT())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.LONG())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.FLOAT())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.DOUBLE())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.CHAR())
    elif _is_instance_of(j_type_info, JBasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO):
        return Types.BASIC_ARRAY(Types.STRING())

    JPickledBytesTypeInfo = gateway.jvm \
        .org.apache.flink.streaming.api.typeinfo.python.PickledByteArrayTypeInfo\
        .PICKLED_BYTE_ARRAY_TYPE_INFO
    if _is_instance_of(j_type_info, JPickledBytesTypeInfo):
        return Types.PICKLED_BYTE_ARRAY()

    JRowTypeInfo = gateway.jvm.org.apache.flink.api.java.typeutils.RowTypeInfo
    if _is_instance_of(j_type_info, JRowTypeInfo):
        j_row_field_names = j_type_info.getFieldNames()
        j_row_field_types = j_type_info.getFieldTypes()
        row_field_types = [_from_java_type(j_row_field_type) for j_row_field_type in
                           j_row_field_types]
        return Types.ROW_NAMED(j_row_field_names, row_field_types)

    JTupleTypeInfo = gateway.jvm.org.apache.flink.api.java.typeutils.TupleTypeInfo
    if _is_instance_of(j_type_info, JTupleTypeInfo):
        j_field_types = []
        for i in range(j_type_info.getArity()):
            j_field_types.append(j_type_info.getTypeAt(i))
        field_types = [_from_java_type(j_field_type) for j_field_type in j_field_types]
        return TupleTypeInfo(field_types)

    raise TypeError("The java type info: %s is not supported in PyFlink currently." % j_type_info)


def _is_instance_of(java_object: JavaObject, java_type: Union[JavaObject, JavaClass]) -> bool:
    if isinstance(java_type, JavaObject):
        return java_object.equals(java_type)
    elif isinstance(java_type, JavaClass):
        return java_object.getClass().isAssignableFrom(java_type._java_lang_class)
    return False
