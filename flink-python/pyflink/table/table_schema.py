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

from pyflink.java_gateway import get_gateway
from pyflink.table.types import _to_java_type, _to_python_type
from pyflink.util.utils import to_jarray

if sys.version >= '3':
    unicode = str


class TableSchema(object):
    """
    A table schema that represents a table's structure with field names and data types.
    """

    def __init__(self, field_names=None, data_types=None, java_object=None):
        if java_object is None:
            gateway = get_gateway()
            j_field_names = to_jarray(gateway.jvm.String, field_names)
            j_data_types = to_jarray(gateway.jvm.TypeInformation,
                                     [_to_java_type(item) for item in data_types])
            self._j_table_schema = gateway.jvm.TableSchema(j_field_names, j_data_types)
        else:
            self._j_table_schema = java_object

    def copy(self):
        """
        Returns a deep copy of the table schema.

        :return: A deep copy of the table schema.
        """
        return TableSchema(java_object=self._j_table_schema.copy())

    def get_field_types(self):
        """
        Returns all field data types as an array.

        .. note::
            Deprecated.
            Use :func:`pyflink.table.table_schema.TableSchema.get_field_data_types` instead.


        :return: A list of all field data types.
        """
        return [_to_python_type(item) for item in self._j_table_schema.getFieldTypes()]

    def get_field_data_types(self):
        """
        Returns all field data types as an array.

        :return: A list of all field data types.
        """
        return [_to_python_type(item) for item in self._j_table_schema.getFieldDataTypes()]

    def get_field_data_type(self, field):
        """
        Returns the specified data type for the given field index or field name.

        :param field: The index of the field or the name of the field.
        :return: The specified data type.
        """
        if not isinstance(field, (int, str, unicode)):
            raise TypeError("not supported data type: %s" % type(field))
        optional_result = self._j_table_schema.getFieldDataType(field)
        if optional_result.isPresent():
            return _to_python_type(optional_result.get())
        else:
            return None

    def get_field_type(self, field):
        """
        Returns the specified data type for the given field index or field name.

        .. note::
            Deprecated.
            Use :func:`pyflink.table.table_schema.TableSchema.get_field_data_type` instead.

        :param field: The index of the field or the name of the field.
        :return: The specified data type.
        """
        if not isinstance(field, (int, str, unicode)):
            raise TypeError("not supported data type: %s" % type(field))
        optional_result = self._j_table_schema.getFieldType(field)
        if optional_result.isPresent():
            return _to_python_type(optional_result.get())
        else:
            return None

    def get_field_count(self):
        """
        Returns the number of fields.

        :return: The number of fields.
        """
        return self._j_table_schema.getFieldCount()

    def get_field_names(self):
        """
        Returns all field names as a list.

        :return: The list of all field names.
        """
        return list(self._j_table_schema.getFieldNames())

    def get_field_name(self, field_index):
        """
        Returns the specified name for the given field index.

        :param field_index: The index of the field.
        :return: The field name.
        """
        optional_result = self._j_table_schema.getFieldName(field_index)
        if optional_result.isPresent():
            return optional_result.get()
        else:
            return None

    def to_row_data_type(self):
        """
        Converts a table schema into a (nested) data type describing a
        :func:`pyflink.table.types.DataTypes.ROW`.

        :return: The row data type.
        """
        return _to_python_type(self._j_table_schema.toRowDataType())

    def to_row_type(self):
        """
        Converts a table schema into a (nested) data type describing a
        :func:`pyflink.table.types.DataTypes.ROW`.

        .. note::
            Deprecated.
            Use :func:`pyflink.table.table_schema.TableSchema.to_row_data_type` instead.

        :return: The row data type.
        """
        return _to_python_type(self._j_table_schema.toRowType())

    def __repr__(self):
        if self._j_table_schema is not None:
            return self._j_table_schema.toString()
        else:
            return super(TableSchema, self).__repr__()

    def __str__(self):
        if self._j_table_schema is not None:
            return self._j_table_schema.toString()
        else:
            return super(TableSchema, self).__str__()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_table_schema == other._j_table_schema

    def __hash__(self):
        if self._j_table_schema is not None:
            return self._j_table_schema.hashCode()
        else:
            return super(TableSchema, self).__hash__()

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def from_type_info(cls, data_type):
        """
        Creates a table schema from a :class:`DataType` instance. If the type information is
        a composite type, the field names and types for the composite type are used to
        construct the :class:`TableSchema` instance. Otherwise, a table schema with a single field
        is created. The field name is "f0" and the field type the provided type.

        ..note::
            Deprecated.
            This method will be removed soon. Use :class:`DataTypes` to declare types.

        :param data_type: The data type from which the table schema is generated.
        :return: The table schema that was generated from the given :class:`DataType`.
        """
        gateway = get_gateway()
        return TableSchema(
            java_object=gateway.jvm.TableSchema.fromTypeInfo(_to_java_type(data_type)))

    @classmethod
    def builder(cls):
        return TableSchema.Builder()

    class Builder(object):
        """
        Builder for creating a :class:`TableSchema`.

        """

        def __init__(self):
            self._field_names = []
            self._field_data_types = []

        def field(self, name, data_type):
            """
            Add a field with name and data type.

            The call order of this method determines the order of fields in the schema.

            :param name: The field name.
            :param data_type: The field data type.
            :return: This object.
            """
            assert name is not None
            assert data_type is not None
            self._field_names.append(name)
            self._field_data_types.append(data_type)
            return self

        def build(self):
            """
            Returns a :class:`TableSchema` instance.

            :return: The :class:`TableSchema` instance.
            """
            return TableSchema(self._field_names, self._field_data_types)
