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
from typing import Optional, TYPE_CHECKING

from pyflink.common import Configuration
from pyflink.common.serialization import BulkWriterFactory, RowDataBulkWriterFactory
from pyflink.datastream.utils import create_hadoop_configuration, create_java_properties
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray

if TYPE_CHECKING:
    from pyflink.table.types import RowType

__all__ = [
    'OrcBulkWriters'
]


class OrcBulkWriters(object):
    """
    Convenient builder to create a :class:`~pyflink.common.serialization.BulkWriterFactory` that
    writes records with a predefined schema into Orc files in a batch fashion.

    Example:
    ::

        >>> row_type = DataTypes.ROW([
        ...     DataTypes.FIELD('string', DataTypes.STRING()),
        ...     DataTypes.FIELD('int_array', DataTypes.ARRAY(DataTypes.INT()))
        ... ])
        >>> sink = FileSink.for_bulk_format(
        ...     OUTPUT_DIR, OrcBulkWriters.for_row_type(
        ...         row_type=row_type,
        ...         writer_properties=Configuration(),
        ...         hadoop_config=Configuration(),
        ...     )
        ... ).build()
        >>> ds.sink_to(sink)

    .. versionadded:: 1.16.0
    """

    @staticmethod
    def for_row_type(row_type: 'RowType',
                     writer_properties: Optional[Configuration] = None,
                     hadoop_config: Optional[Configuration] = None) \
            -> BulkWriterFactory:
        """
        Create a :class:`~pyflink.common.serialization.BulkWriterFactory` that writes records
        with a predefined schema into Orc files in a batch fashion.

        :param row_type: The RowType of records, it should match the RowTypeInfo of Row records.
        :param writer_properties: Orc writer options.
        :param hadoop_config: Hadoop configuration.
        """
        from pyflink.table.types import RowType
        if not isinstance(row_type, RowType):
            raise TypeError('row_type must be an instance of RowType')

        from pyflink.table.types import _to_java_data_type
        j_data_type = _to_java_data_type(row_type)
        jvm = get_gateway().jvm
        j_row_type = j_data_type.getLogicalType()
        orc_types = to_jarray(
            jvm.org.apache.flink.table.types.logical.LogicalType,
            [i for i in j_row_type.getChildren()]
        )
        type_description = jvm.org.apache.flink.orc \
            .OrcSplitReaderUtil.logicalTypeToOrcType(j_row_type)
        if writer_properties is None:
            writer_properties = Configuration()
        if hadoop_config is None:
            hadoop_config = Configuration()

        return RowDataBulkWriterFactory(
            jvm.org.apache.flink.orc.writer.OrcBulkWriterFactory(
                jvm.org.apache.flink.orc.vector.RowDataVectorizer(
                    type_description.toString(),
                    orc_types
                ),
                create_java_properties(writer_properties),
                create_hadoop_configuration(hadoop_config)
            ),
            row_type
        )
