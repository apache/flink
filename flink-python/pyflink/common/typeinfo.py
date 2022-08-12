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
from enum import Enum
from typing import List, Union

from py4j.java_gateway import JavaClass, JavaObject

from pyflink.java_gateway import get_gateway

__all__ = ['TypeInformation', 'Types']


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
        pass

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


class BasicType(Enum):
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
    INSTANT = "Instant"


class BasicTypeInfo(TypeInformation):
    """
    Type information for primitive types (int, long, double, byte, ...), String, BigInteger,
    and BigDecimal.
    """

    def __init__(self, basic_type: BasicType):
        self._basic_type = basic_type
        super(BasicTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            JBasicTypeInfo = get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo
            if self._basic_type == BasicType.STRING:
                self._j_typeinfo = JBasicTypeInfo.STRING_TYPE_INFO
            elif self._basic_type == BasicType.BYTE:
                self._j_typeinfo = JBasicTypeInfo.BYTE_TYPE_INFO
            elif self._basic_type == BasicType.BOOLEAN:
                self._j_typeinfo = JBasicTypeInfo.BOOLEAN_TYPE_INFO
            elif self._basic_type == BasicType.SHORT:
                self._j_typeinfo = JBasicTypeInfo.SHORT_TYPE_INFO
            elif self._basic_type == BasicType.INT:
                self._j_typeinfo = JBasicTypeInfo.INT_TYPE_INFO
            elif self._basic_type == BasicType.LONG:
                self._j_typeinfo = JBasicTypeInfo.LONG_TYPE_INFO
            elif self._basic_type == BasicType.FLOAT:
                self._j_typeinfo = JBasicTypeInfo.FLOAT_TYPE_INFO
            elif self._basic_type == BasicType.DOUBLE:
                self._j_typeinfo = JBasicTypeInfo.DOUBLE_TYPE_INFO
            elif self._basic_type == BasicType.CHAR:
                self._j_typeinfo = JBasicTypeInfo.CHAR_TYPE_INFO
            elif self._basic_type == BasicType.BIG_INT:
                self._j_typeinfo = JBasicTypeInfo.BIG_INT_TYPE_INFO
            elif self._basic_type == BasicType.BIG_DEC:
                self._j_typeinfo = JBasicTypeInfo.BIG_DEC_TYPE_INFO
            elif self._basic_type == BasicType.INSTANT:
                self._j_typeinfo = JBasicTypeInfo.INSTANT_TYPE_INFO
            else:
                raise TypeError("Invalid BasicType %s." % self._basic_type)
        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if isinstance(o, BasicTypeInfo):
            return self._basic_type == o._basic_type
        return False

    def __repr__(self):
        return self._basic_type.value

    @staticmethod
    def STRING_TYPE_INFO():
        return BasicTypeInfo(BasicType.STRING)

    @staticmethod
    def BOOLEAN_TYPE_INFO():
        return BasicTypeInfo(BasicType.BOOLEAN)

    @staticmethod
    def BYTE_TYPE_INFO():
        return BasicTypeInfo(BasicType.BYTE)

    @staticmethod
    def SHORT_TYPE_INFO():
        return BasicTypeInfo(BasicType.SHORT)

    @staticmethod
    def INT_TYPE_INFO():
        return BasicTypeInfo(BasicType.INT)

    @staticmethod
    def LONG_TYPE_INFO():
        return BasicTypeInfo(BasicType.LONG)

    @staticmethod
    def FLOAT_TYPE_INFO():
        return BasicTypeInfo(BasicType.FLOAT)

    @staticmethod
    def DOUBLE_TYPE_INFO():
        return BasicTypeInfo(BasicType.DOUBLE)

    @staticmethod
    def CHAR_TYPE_INFO():
        return BasicTypeInfo(BasicType.CHAR)

    @staticmethod
    def BIG_INT_TYPE_INFO():
        return BasicTypeInfo(BasicType.BIG_INT)

    @staticmethod
    def BIG_DEC_TYPE_INFO():
        return BasicTypeInfo(BasicType.BIG_DEC)

    @staticmethod
    def INSTANT_TYPE_INFO():
        return InstantTypeInfo(BasicType.INSTANT)


class InstantTypeInfo(BasicTypeInfo):
    """
    InstantTypeInfo enables users to get Instant TypeInfo.
    """
    def __init__(self, basic_type: BasicType):
        super(InstantTypeInfo, self).__init__(basic_type)

    def need_conversion(self):
        return True

    def to_internal_type(self, obj):
        return obj.to_epoch_milli() * 1000

    def from_internal_type(self, obj):
        from pyflink.common.time import Instant
        return Instant.of_epoch_milli(obj // 1000)


class SqlTimeTypeInfo(TypeInformation):
    """
    SqlTimeTypeInfo enables users to get Sql Time TypeInfo.
    """

    @staticmethod
    def DATE():
        return DateTypeInfo()

    @staticmethod
    def TIME():
        return TimeTypeInfo()

    @staticmethod
    def TIMESTAMP():
        return TimestampTypeInfo()


class PrimitiveArrayTypeInfo(TypeInformation):
    """
    A TypeInformation for arrays of primitive types (int, long, double, ...).
    Supports the creation of dedicated efficient serializers for these types.
    """

    def __init__(self, element_type: TypeInformation):
        self._element_type = element_type
        super(PrimitiveArrayTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            JPrimitiveArrayTypeInfo = get_gateway().jvm.org.apache.flink.api.common.typeinfo \
                .PrimitiveArrayTypeInfo
            if self._element_type == Types.BOOLEAN():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.BYTE():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.SHORT():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.INT():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.LONG():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.FLOAT():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.DOUBLE():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
            elif self._element_type == Types.CHAR():
                self._j_typeinfo = JPrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO
            else:
                raise TypeError("Invalid element type for a primitive array.")
        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if isinstance(o, PrimitiveArrayTypeInfo):
            return self._element_type == o._element_type
        return False

    def __repr__(self) -> str:
        return "PrimitiveArrayTypeInfo<%s>" % self._element_type


class BasicArrayTypeInfo(TypeInformation):
    """
    A TypeInformation for arrays of boxed primitive types (Integer, Long, Double, ...).
    Supports the creation of dedicated efficient serializers for these types.
    """

    def __init__(self, element_type: TypeInformation):
        self._element_type = element_type
        super(BasicArrayTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            JBasicArrayTypeInfo = get_gateway().jvm.org.apache.flink.api.common.typeinfo \
                .BasicArrayTypeInfo
            if self._element_type == Types.BOOLEAN():
                self._j_typeinfo = JBasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
            elif self._element_type == Types.BYTE():
                self._j_typeinfo = JBasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO
            elif self._element_type == Types.SHORT():
                self._j_typeinfo = JBasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
            elif self._element_type == Types.INT():
                self._j_typeinfo = JBasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
            elif self._element_type == Types.LONG():
                self._j_typeinfo = JBasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
            elif self._element_type == Types.FLOAT():
                self._j_typeinfo = JBasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
            elif self._element_type == Types.DOUBLE():
                self._j_typeinfo = JBasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
            elif self._element_type == Types.CHAR():
                self._j_typeinfo = JBasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
            elif self._element_type == Types.STRING():
                self._j_typeinfo = JBasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
            else:
                raise TypeError("Invalid element type for a primitive array.")

        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if isinstance(o, BasicArrayTypeInfo):
            return self._element_type == o._element_type
        return False

    def __repr__(self):
        return "BasicArrayTypeInfo<%s>" % self._element_type


class ObjectArrayTypeInfo(TypeInformation):
    """
    A TypeInformation for arrays of non-primitive types.
    """

    def __init__(self, element_type: TypeInformation):
        self._element_type = element_type
        super(ObjectArrayTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            JTypes = get_gateway().jvm.org.apache.flink.api.common.typeinfo.Types
            self._j_typeinfo = JTypes.OBJECT_ARRAY(self._element_type.get_java_type_info())

        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if isinstance(o, ObjectArrayTypeInfo):
            return self._element_type == o._element_type
        return False

    def __repr__(self):
        return "ObjectArrayTypeInfo<%s>" % self._element_type


class PickledBytesTypeInfo(TypeInformation):
    """
    A PickledBytesTypeInfo indicates the data is a primitive byte array generated by pickle
    serializer.
    """

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm.org.apache.flink.streaming.api.typeinfo.python\
                .PickledByteArrayTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, PickledBytesTypeInfo)

    def __repr__(self):
        return "PickledByteArrayTypeInfo"


class RowTypeInfo(TypeInformation):
    """
    TypeInformation for Row.
    """

    def __init__(self, field_types: List[TypeInformation], field_names: List[str] = None):
        self._field_types = field_types
        self._field_names = field_names
        self._need_conversion = [f.need_conversion() if isinstance(f, TypeInformation) else None
                                 for f in self._field_types]
        self._need_serialize_any_field = any(self._need_conversion)
        super(RowTypeInfo, self).__init__()

    def get_field_names(self) -> List[str]:
        if not self._field_names:
            j_field_names = self.get_java_type_info().getFieldNames()
            self._field_names = [name for name in j_field_names]
        return self._field_names

    def get_field_index(self, field_name: str) -> int:
        if self._field_names:
            return self._field_names.index(field_name)
        return -1

    def get_field_types(self) -> List[TypeInformation]:
        return self._field_types

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            j_types_array = get_gateway()\
                .new_array(get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation,
                           len(self._field_types))
            for i in range(len(self._field_types)):
                field_type = self._field_types[i]
                if isinstance(field_type, TypeInformation):
                    j_types_array[i] = field_type.get_java_type_info()

            if self._field_names is None:
                self._j_typeinfo = get_gateway().jvm\
                    .org.apache.flink.api.java.typeutils.RowTypeInfo(j_types_array)
            else:
                j_names_array = get_gateway().new_array(get_gateway().jvm.java.lang.String,
                                                        len(self._field_names))
                for i in range(len(self._field_names)):
                    j_names_array[i] = self._field_names[i]
                self._j_typeinfo = get_gateway().jvm\
                    .org.apache.flink.api.java.typeutils.RowTypeInfo(j_types_array, j_names_array)
        return self._j_typeinfo

    def __eq__(self, other) -> bool:
        if isinstance(other, RowTypeInfo):
            return self._field_types == other._field_types
        return False

    def __repr__(self) -> str:
        if self._field_names:
            return "RowTypeInfo(%s)" % ', '.join([field_name + ': ' + str(field_type)
                                                  for field_name, field_type in
                                                  zip(self.get_field_names(),
                                                      self.get_field_types())])
        else:
            return "RowTypeInfo(%s)" % ', '.join(
                [str(field_type) for field_type in self._field_types])

    def need_conversion(self):
        return True

    def to_internal_type(self, obj):
        if obj is None:
            return
        from pyflink.common import Row, RowKind
        if self._need_serialize_any_field:
            # Only calling to_internal_type function for fields that need conversion
            if isinstance(obj, dict):
                return (RowKind.INSERT.value,) + tuple(
                    f.to_internal_type(obj.get(n)) if c else obj.get(n)
                    for n, f, c in
                    zip(self.get_field_names(), self._field_types, self._need_conversion))
            elif isinstance(obj, Row) and hasattr(obj, "_fields"):
                return (obj.get_row_kind().value,) + tuple(
                    f.to_internal_type(obj[n]) if c else obj[n]
                    for n, f, c in
                    zip(self.get_field_names(), self._field_types, self._need_conversion))
            elif isinstance(obj, Row):
                return (obj.get_row_kind().value,) + tuple(
                    f.to_internal_type(v) if c else v
                    for f, v, c in zip(self._field_types, obj, self._need_conversion))
            elif isinstance(obj, (tuple, list)):
                return (RowKind.INSERT.value,) + tuple(
                    f.to_internal_type(v) if c else v
                    for f, v, c in zip(self._field_types, obj, self._need_conversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return (RowKind.INSERT.value,) + tuple(
                    f.to_internal_type(d.get(n)) if c else d.get(n)
                    for n, f, c in
                    zip(self.get_field_names(), self._field_types, self._need_conversion))
            else:
                raise ValueError("Unexpected tuple %r with RowTypeInfo" % obj)
        else:
            if isinstance(obj, dict):
                return (RowKind.INSERT.value,) + tuple(obj.get(n) for n in self.get_field_names())
            elif isinstance(obj, Row) and hasattr(obj, "_fields"):
                return (obj.get_row_kind().value,) + tuple(
                    obj[n] for n in self.get_field_names())
            elif isinstance(obj, Row):
                return (obj.get_row_kind().value,) + tuple(obj)
            elif isinstance(obj, (list, tuple)):
                return (RowKind.INSERT.value,) + tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return (RowKind.INSERT.value,) + tuple(d.get(n) for n in self.get_field_names())
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
                      for f, v, c in zip(self._field_types, obj, self._need_conversion)]
        else:
            values = obj
        return tuple(values)


class TupleTypeInfo(TypeInformation):
    """
    TypeInformation for Tuple.
    """

    def __init__(self, field_types: List[TypeInformation]):
        self._field_types = field_types
        self._need_conversion = [f.need_conversion() if isinstance(f, TypeInformation) else None
                                 for f in self._field_types]
        self._need_serialize_any_field = any(self._need_conversion)
        super(TupleTypeInfo, self).__init__()

    def get_field_types(self) -> List[TypeInformation]:
        return self._field_types

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            j_types_array = get_gateway().new_array(
                get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation,
                len(self._field_types))

            for i in range(len(self._field_types)):
                field_type = self._field_types[i]
                if isinstance(field_type, TypeInformation):
                    j_types_array[i] = field_type.get_java_type_info()

            self._j_typeinfo = get_gateway().jvm \
                .org.apache.flink.api.java.typeutils.TupleTypeInfo(j_types_array)
        return self._j_typeinfo

    def need_conversion(self):
        return True

    def to_internal_type(self, obj):
        if obj is None:
            return
        from pyflink.common import Row
        if self._need_serialize_any_field:
            # Only calling to_internal_type function for fields that need conversion
            if isinstance(obj, (list, tuple, Row)):
                return tuple(
                    f.to_internal_type(v) if c else v
                    for f, v, c in zip(self._field_types, obj, self._need_conversion))
            else:
                raise ValueError("Unexpected tuple %r with TupleTypeInfo" % obj)
        else:
            if isinstance(obj, (list, tuple, Row)):
                return tuple(obj)
            else:
                raise ValueError("Unexpected tuple %r with TupleTypeInfo" % obj)

    def from_internal_type(self, obj):
        if obj is None or isinstance(obj, (tuple, list)):
            # it's already converted by pickler
            return obj
        if self._need_serialize_any_field:
            # Only calling from_internal_type function for fields that need conversion
            values = [f.from_internal_type(v) if c else v
                      for f, v, c in zip(self._field_types, obj, self._need_conversion)]
        else:
            values = obj
        return tuple(values)

    def __eq__(self, other) -> bool:
        if isinstance(other, TupleTypeInfo):
            return self._field_types == other._field_types
        return False

    def __repr__(self) -> str:
        return "TupleTypeInfo(%s)" % ', '.join(
            [str(field_type) for field_type in self._field_types])


class DateTypeInfo(TypeInformation):
    """
    TypeInformation for Date.
    """

    def __init__(self):
        super(DateTypeInfo, self).__init__()

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
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.DATE
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DateTypeInfo)

    def __repr__(self):
        return "DateTypeInfo"


class TimeTypeInfo(TypeInformation):
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
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIME
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimeTypeInfo)

    def __repr__(self) -> str:
        return "TimeTypeInfo"


class TimestampTypeInfo(TypeInformation):
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
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP
        return self._j_typeinfo

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimestampTypeInfo)

    def __repr__(self):
        return "TimestampTypeInfo"


class ListTypeInfo(TypeInformation):
    """
    A TypeInformation for the list types.
    """

    def __init__(self, element_type: TypeInformation):
        self.elem_type = element_type
        super(ListTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.Types.LIST(
                self.elem_type.get_java_type_info())
        return self._j_typeinfo

    def __eq__(self, other):
        if isinstance(other, ListTypeInfo):
            return self.elem_type == other.elem_type
        else:
            return False

    def __repr__(self):
        return "ListTypeInfo<%s>" % self.elem_type


class MapTypeInfo(TypeInformation):

    def __init__(self, key_type_info: TypeInformation, value_type_info: TypeInformation):
        self._key_type_info = key_type_info
        self._value_type_info = value_type_info
        super(MapTypeInfo, self).__init__()

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            self._j_typeinfo = get_gateway().jvm\
                .org.apache.flink.api.common.typeinfo.Types.MAP(
                self._key_type_info.get_java_type_info(),
                self._value_type_info.get_java_type_info())
        return self._j_typeinfo

    def __eq__(self, other):
        if isinstance(other, MapTypeInfo):
            return self._key_type_info == other._key_type_info and \
                self._value_type_info == other._value_type_info

    def __repr__(self) -> str:
        return 'MapTypeInfo<{}, {}>'.format(self._key_type_info, self._value_type_info)


class ExternalTypeInfo(TypeInformation):
    def __init__(self, type_info: TypeInformation):
        super(ExternalTypeInfo, self).__init__()
        self._type_info = type_info

    def get_java_type_info(self) -> JavaObject:
        if not self._j_typeinfo:
            gateway = get_gateway()
            TypeInfoDataTypeConverter = \
                gateway.jvm.org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
            JExternalTypeInfo = \
                gateway.jvm.org.apache.flink.table.runtime.typeutils.ExternalTypeInfo

            j_data_type = TypeInfoDataTypeConverter.toDataType(self._type_info.get_java_type_info())
            self._j_typeinfo = JExternalTypeInfo.of(j_data_type)
        return self._j_typeinfo

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._type_info == other._type_info

    def __repr__(self):
        return 'ExternalTypeInfo<{}>'.format(self._type_info)


class Types(object):
    """
    This class gives access to the type information of the most common types for which Flink has
    built-in serializers and comparators.
    """

    @staticmethod
    def STRING() -> TypeInformation:
        """
        Returns type information for string. Supports a None value.
        """
        return BasicTypeInfo.STRING_TYPE_INFO()

    @staticmethod
    def BYTE() -> TypeInformation:
        """
        Returns type information for byte. Does not support a None value.
        """
        return BasicTypeInfo.BYTE_TYPE_INFO()

    @staticmethod
    def BOOLEAN() -> TypeInformation:
        """
        Returns type information for bool. Does not support a None value.
        """
        return BasicTypeInfo.BOOLEAN_TYPE_INFO()

    @staticmethod
    def SHORT() -> TypeInformation:
        """
        Returns type information for short. Does not support a None value.
        """
        return BasicTypeInfo.SHORT_TYPE_INFO()

    @staticmethod
    def INT() -> TypeInformation:
        """
        Returns type information for int. Does not support a None value.
        """
        return BasicTypeInfo.INT_TYPE_INFO()

    @staticmethod
    def LONG() -> TypeInformation:
        """
        Returns type information for long. Does not support a None value.
        """
        return BasicTypeInfo.LONG_TYPE_INFO()

    @staticmethod
    def FLOAT() -> TypeInformation:
        """
        Returns type information for float. Does not support a None value.
        """
        return BasicTypeInfo.FLOAT_TYPE_INFO()

    @staticmethod
    def DOUBLE() -> TypeInformation:
        """
        Returns type information for double. Does not support a None value.
        """
        return BasicTypeInfo.DOUBLE_TYPE_INFO()

    @staticmethod
    def CHAR() -> TypeInformation:
        """
        Returns type information for char. Does not support a None value.
        """
        return BasicTypeInfo.CHAR_TYPE_INFO()

    @staticmethod
    def BIG_INT() -> TypeInformation:
        """
        Returns type information for BigInteger. Supports a None value.
        """
        return BasicTypeInfo.BIG_INT_TYPE_INFO()

    @staticmethod
    def BIG_DEC() -> TypeInformation:
        """
        Returns type information for BigDecimal. Supports a None value.
        """
        return BasicTypeInfo.BIG_DEC_TYPE_INFO()

    @staticmethod
    def INSTANT() -> TypeInformation:
        """
        Returns type information for Instant. Supports a None value.
        """
        return BasicTypeInfo.INSTANT_TYPE_INFO()

    @staticmethod
    def SQL_DATE() -> TypeInformation:
        """
        Returns type information for Date. Supports a None value.
        """
        return SqlTimeTypeInfo.DATE()

    @staticmethod
    def SQL_TIME() -> TypeInformation:
        """
        Returns type information for Time. Supports a None value.
        """
        return SqlTimeTypeInfo.TIME()

    @staticmethod
    def SQL_TIMESTAMP() -> TypeInformation:
        """
        Returns type information for Timestamp. Supports a None value.
        """
        return SqlTimeTypeInfo.TIMESTAMP()

    @staticmethod
    def PICKLED_BYTE_ARRAY() -> TypeInformation:
        """
        Returns type information which uses pickle for serialization/deserialization.
        """
        return PickledBytesTypeInfo()

    @staticmethod
    def ROW(field_types: List[TypeInformation]):
        """
        Returns type information for Row with fields of the given types. A row itself must not be
        null.

        :param field_types: the types of the row fields, e.g., Types.String(), Types.INT()
        """
        return RowTypeInfo(field_types)

    @staticmethod
    def ROW_NAMED(field_names: List[str], field_types: List[TypeInformation]):
        """
        Returns type information for Row with fields of the given types and with given names. A row
        must not be null.

        :param field_names: array of field names.
        :param field_types: array of field types.
        """
        return RowTypeInfo(field_types, field_names)

    @staticmethod
    def TUPLE(field_types: List[TypeInformation]):
        """
        Returns type information for Tuple with fields of the given types. A Tuple itself must not
        be null.

        :param field_types: array of field types.
        """
        return TupleTypeInfo(field_types)

    @staticmethod
    def PRIMITIVE_ARRAY(element_type: TypeInformation):
        """
        Returns type information for arrays of primitive type (such as byte[]). The array must not
        be null.

        :param element_type: element type of the array (e.g. Types.BOOLEAN(), Types.INT(),
                             Types.DOUBLE())
        """
        return PrimitiveArrayTypeInfo(element_type)

    @staticmethod
    def BASIC_ARRAY(element_type: TypeInformation) -> TypeInformation:
        """
        Returns type information for arrays of boxed primitive type (such as Integer[]).

        :param element_type: element type of the array (e.g. Types.BOOLEAN(), Types.INT(),
                             Types.DOUBLE())
        """
        return BasicArrayTypeInfo(element_type)

    @staticmethod
    def OBJECT_ARRAY(element_type: TypeInformation) -> TypeInformation:
        """
        Returns type information for arrays of non-primitive types. The array itself must not be
        None. None values for elements are supported.

        :param element_type: element type of the array
        """
        return ObjectArrayTypeInfo(element_type)

    @staticmethod
    def MAP(key_type_info: TypeInformation, value_type_info: TypeInformation) -> TypeInformation:
        """
        Special TypeInformation used by MapStateDescriptor

        :param key_type_info: Element type of key (e.g. Types.BOOLEAN(), Types.INT(),
                              Types.DOUBLE())
        :param value_type_info: Element type of value (e.g. Types.BOOLEAN(), Types.INT(),
                                Types.DOUBLE())
        """
        return MapTypeInfo(key_type_info, value_type_info)

    @staticmethod
    def LIST(element_type_info: TypeInformation) -> TypeInformation:
        """
        A TypeInformation for the list type.

        :param element_type_info: The type of the elements in the list
        """
        return ListTypeInfo(element_type_info)


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
    elif _is_instance_of(j_type_info, JBasicTypeInfo.INSTANT_TYPE_INFO):
        return Types.INSTANT()

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

    JObjectArrayTypeInfo = gateway.jvm.org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
    if _is_instance_of(j_type_info, JObjectArrayTypeInfo):
        return Types.OBJECT_ARRAY(_from_java_type(j_type_info.getComponentInfo()))

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
        row_field_names = [field_name for field_name in j_row_field_names]
        return Types.ROW_NAMED(row_field_names, row_field_types)

    JTupleTypeInfo = gateway.jvm.org.apache.flink.api.java.typeutils.TupleTypeInfo
    if _is_instance_of(j_type_info, JTupleTypeInfo):
        j_field_types = []
        for i in range(j_type_info.getArity()):
            j_field_types.append(j_type_info.getTypeAt(i))
        field_types = [_from_java_type(j_field_type) for j_field_type in j_field_types]
        return TupleTypeInfo(field_types)

    JMapTypeInfo = get_gateway().jvm.org.apache.flink.api.java.typeutils.MapTypeInfo
    if _is_instance_of(j_type_info, JMapTypeInfo):
        j_key_type_info = j_type_info.getKeyTypeInfo()
        j_value_type_info = j_type_info.getValueTypeInfo()
        return MapTypeInfo(_from_java_type(j_key_type_info), _from_java_type(j_value_type_info))

    JListTypeInfo = get_gateway().jvm.org.apache.flink.api.java.typeutils.ListTypeInfo
    if _is_instance_of(j_type_info, JListTypeInfo):
        j_element_type_info = j_type_info.getElementTypeInfo()
        return ListTypeInfo(_from_java_type(j_element_type_info))

    JExternalTypeInfo = gateway.jvm.org.apache.flink.table.runtime.typeutils.ExternalTypeInfo
    if _is_instance_of(j_type_info, JExternalTypeInfo):
        TypeInfoDataTypeConverter = \
            gateway.jvm.org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
        return ExternalTypeInfo(_from_java_type(
            TypeInfoDataTypeConverter.toLegacyTypeInfo(j_type_info.getDataType())))

    raise TypeError("The java type info: %s is not supported in PyFlink currently." % j_type_info)


def _is_instance_of(java_object: JavaObject, java_type: Union[JavaObject, JavaClass]) -> bool:
    if isinstance(java_type, JavaObject):
        return java_object.equals(java_type)
    elif isinstance(java_type, JavaClass):
        return java_object.getClass().isAssignableFrom(java_type._java_lang_class)
    return False
