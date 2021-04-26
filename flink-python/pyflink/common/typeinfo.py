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

from abc import ABC
from typing import List, Union

from py4j.java_gateway import JavaClass, JavaObject

from pyflink.java_gateway import get_gateway


class TypeInformation(ABC):
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


class WrapperTypeInfo(TypeInformation):
    """
    A wrapper class for java TypeInformation Objects.
    """

    def __init__(self, j_typeinfo):
        self._j_typeinfo = j_typeinfo

    def get_java_type_info(self) -> JavaObject:
        return self._j_typeinfo

    def __eq__(self, o) -> bool:
        if type(o) is type(self):
            return self._j_typeinfo.equals(o._j_typeinfo)
        else:
            return False

    def __hash__(self) -> int:
        return hash(self._j_typeinfo)

    def __str__(self):
        return self._j_typeinfo.toString()


class BasicTypeInfo(TypeInformation, ABC):
    """
    Type information for primitive types (int, long, double, byte, ...), String, BigInteger,
    and BigDecimal.
    """

    @staticmethod
    def STRING_TYPE_INFO():
        return WrapperTypeInfo(get_gateway().jvm
                               .org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO)

    @staticmethod
    def BOOLEAN_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO)

    @staticmethod
    def BYTE_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO)

    @staticmethod
    def SHORT_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO)

    @staticmethod
    def INT_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO)

    @staticmethod
    def LONG_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO)

    @staticmethod
    def FLOAT_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO)

    @staticmethod
    def DOUBLE_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO)

    @staticmethod
    def CHAR_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.CHAR_TYPE_INFO)

    @staticmethod
    def BIG_INT_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_INT_TYPE_INFO)

    @staticmethod
    def BIG_DEC_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO)


class SqlTimeTypeInfo(TypeInformation, ABC):
    """
    SqlTimeTypeInfo enables users to get Sql Time TypeInfo.
    """

    @staticmethod
    def DATE():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.DATE)

    @staticmethod
    def TIME():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIME)

    @staticmethod
    def TIMESTAMP():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP)


class PrimitiveArrayTypeInfo(TypeInformation, ABC):
    """
    A TypeInformation for arrays of primitive types (int, long, double, ...).
    Supports the creation of dedicated efficient serializers for these types.
    """

    @staticmethod
    def BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def BYTE_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def SHORT_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def INT_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def LONG_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def FLOAT_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)

    @staticmethod
    def CHAR_PRIMITIVE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo
            .PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)


class PickledBytesTypeInfo(TypeInformation, ABC):
    """
    A PickledBytesTypeInfo indicates the data is a primitive byte array generated by pickle
    serializer.
    """

    @staticmethod
    def PICKLED_BYTE_ARRAY_TYPE_INFO():
        return WrapperTypeInfo(get_gateway().jvm.org.apache.flink.streaming.api.typeinfo.python
                               .PickledByteArrayTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO)


class RowTypeInfo(WrapperTypeInfo):
    """
    TypeInformation for Row.
    """

    def __init__(self, types: List[TypeInformation], field_names: List[str] = None):
        self.types = types
        self.field_names = field_names
        self.j_types_array = get_gateway().new_array(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation, len(types))
        for i in range(len(types)):
            self.j_types_array[i] = types[i].get_java_type_info()

        if field_names is None:
            self._j_typeinfo = get_gateway().jvm.org.apache.flink.api.java.typeutils.RowTypeInfo(
                self.j_types_array)
        else:
            j_names_array = get_gateway().new_array(get_gateway().jvm.java.lang.String,
                                                    len(field_names))
            for i in range(len(field_names)):
                j_names_array[i] = field_names[i]
            self._j_typeinfo = get_gateway().jvm.org.apache.flink.api.java.typeutils.RowTypeInfo(
                self.j_types_array, j_names_array)
        super(RowTypeInfo, self).__init__(self._j_typeinfo)

    def get_field_names(self) -> List[str]:
        j_field_names = self._j_typeinfo.getFieldNames()
        field_names = [name for name in j_field_names]
        return field_names

    def get_field_index(self, field_name: str) -> int:
        return self._j_typeinfo.getFieldIndex(field_name)

    def get_field_types(self) -> List[TypeInformation]:
        return self.types

    def __eq__(self, other) -> bool:
        return self._j_typeinfo.equals(other._j_typeinfo)

    def __hash__(self) -> int:
        return self._j_typeinfo.hashCode()

    def __str__(self) -> str:

        return "RowTypeInfo(%s)" % ', '.join([field_name + ': ' + field_type.__str__()
                                              for field_name, field_type in
                                              zip(self.get_field_names(),
                                                  self.get_field_types())])


class TupleTypeInfo(WrapperTypeInfo):
    """
    TypeInformation for Tuple.
    """

    def __init__(self, types: List[TypeInformation]):
        self.types = types
        j_types_array = get_gateway().new_array(
            get_gateway().jvm.org.apache.flink.api.common.typeinfo.TypeInformation, len(types))

        for i in range(len(types)):
            j_types_array[i] = types[i].get_java_type_info()

        j_typeinfo = get_gateway().jvm \
            .org.apache.flink.api.java.typeutils.TupleTypeInfo(j_types_array)
        super(TupleTypeInfo, self).__init__(j_typeinfo=j_typeinfo)

    def get_field_types(self) -> List[TypeInformation]:
        return self.types

    def __eq__(self, other) -> bool:
        return self._j_typeinfo.equals(other._j_typeinfo)

    def __hash__(self) -> int:
        return self._j_typeinfo.hashCode()

    def __str__(self) -> str:
        return "TupleTypeInfo(%s)" % ', '.join([field_type.__str__() for field_type in self.types])


class Types(object):
    """
    This class gives access to the type information of the most common types for which Flink has
    built-in serializers and comparators.
    """

    STRING = BasicTypeInfo.STRING_TYPE_INFO
    BYTE = BasicTypeInfo.BYTE_TYPE_INFO
    BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO
    SHORT = BasicTypeInfo.SHORT_TYPE_INFO
    INT = BasicTypeInfo.INT_TYPE_INFO
    LONG = BasicTypeInfo.LONG_TYPE_INFO
    FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO
    DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO
    CHAR = BasicTypeInfo.CHAR_TYPE_INFO
    BIG_INT = BasicTypeInfo.BIG_INT_TYPE_INFO
    BIG_DEC = BasicTypeInfo.BIG_DEC_TYPE_INFO

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
        if element_type == Types.BOOLEAN():
            return PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.BYTE():
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.SHORT():
            return PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.INT():
            return PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.LONG():
            return PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.FLOAT():
            return PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.DOUBLE():
            return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO()
        elif element_type == Types.CHAR():
            return PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO()
        else:
            raise TypeError("Invalid element type for a primitive array.")


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
