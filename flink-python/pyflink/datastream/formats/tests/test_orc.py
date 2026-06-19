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
import glob
import os
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import List, Tuple

import pandas as pd

from pyflink.common import Row
from pyflink.common.typeinfo import RowTypeInfo, Types
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.formats.orc import OrcBulkWriters
from pyflink.datastream.formats.tests.test_parquet import _create_parquet_array_row_and_data, \
    _check_parquet_array_results, _create_parquet_map_row_and_data, _check_parquet_map_results
from pyflink.java_gateway import get_gateway
from pyflink.table.types import RowType, DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase, to_java_data_structure


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Orc format tests')
class FileSinkOrcBulkWriterTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.env.set_parallelism(1)
        self.orc_dir_name = tempfile.mkdtemp(dir=self.tempdir)

    def test_orc_basic_write(self):
        row_type, row_type_info, data = _create_orc_basic_row_and_data()
        self._build_orc_job(row_type, row_type_info, data)
        self.env.execute('test_orc_basic_write')
        results = self._read_orc_file()
        _check_orc_basic_results(self, results)

    def test_orc_array_write(self):
        row_type, row_type_info, data = _create_parquet_array_row_and_data()
        self._build_orc_job(row_type, row_type_info, data)
        self.env.execute()
        results = self._read_orc_file()
        _check_parquet_array_results(self, results)

    def test_orc_map_write(self):
        row_type, row_type_info, data = _create_parquet_map_row_and_data()
        self._build_orc_job(row_type, row_type_info, data)
        self.env.execute()
        results = self._read_orc_file()
        _check_parquet_map_results(self, results)

    def _build_orc_job(self, row_type: RowType, row_type_info: RowTypeInfo, data: List[Row]):
        jvm = get_gateway().jvm
        sink = FileSink.for_bulk_format(
            self.orc_dir_name, OrcBulkWriters.for_row_type(row_type)
        ).build()
        j_list = jvm.java.util.ArrayList()
        for d in data:
            j_list.add(to_java_data_structure(d))
        ds = DataStream(self.env._j_stream_execution_environment.fromCollection(
            j_list,
            row_type_info.get_java_type_info()
        ))
        ds.sink_to(sink)

    def _read_orc_file(self):
        records = []
        for file in glob.glob(os.path.join(os.path.join(self.orc_dir_name, '**/*'))):
            df = pd.read_orc(file)
            for i in range(df.shape[0]):
                records.append(df.loc[i])
        return records


def _create_orc_basic_row_and_data() -> Tuple[RowType, RowTypeInfo, List[Row]]:
    row_type = DataTypes.ROW([
        DataTypes.FIELD('char', DataTypes.CHAR(10)),
        DataTypes.FIELD('varchar', DataTypes.VARCHAR(10)),
        DataTypes.FIELD('bytes', DataTypes.BYTES()),
        DataTypes.FIELD('boolean', DataTypes.BOOLEAN()),
        DataTypes.FIELD('decimal', DataTypes.DECIMAL(2, 0)),
        DataTypes.FIELD('int', DataTypes.INT()),
        DataTypes.FIELD('bigint', DataTypes.BIGINT()),
        DataTypes.FIELD('double', DataTypes.DOUBLE()),
        DataTypes.FIELD('date', DataTypes.DATE().bridged_to('java.sql.Date')),
        DataTypes.FIELD('timestamp', DataTypes.TIMESTAMP(3).bridged_to('java.sql.Timestamp')),
    ])
    row_type_info = Types.ROW_NAMED(
        ['char', 'varchar', 'bytes', 'boolean', 'decimal', 'int', 'bigint', 'double',
         'date', 'timestamp'],
        [Types.STRING(), Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE()), Types.BOOLEAN(),
         Types.BIG_DEC(), Types.INT(), Types.LONG(), Types.DOUBLE(), Types.SQL_DATE(),
         Types.SQL_TIMESTAMP()]
    )
    data = [Row(
        char='char',
        varchar='varchar',
        bytes=b'varbinary',
        boolean=True,
        decimal=Decimal(1.5),
        int=2147483647,
        bigint=-9223372036854775808,
        double=2e-308,
        date=date(1970, 1, 1),
        timestamp=datetime(1970, 1, 2, 3, 4, 5, 600000),
    )]
    return row_type, row_type_info, data


def _check_orc_basic_results(test, results):
    row = results[0]
    test.assertEqual(row['char'], b'char      ')
    test.assertEqual(row['varchar'], 'varchar')
    test.assertEqual(row['bytes'], b'varbinary')
    test.assertEqual(row['boolean'], True)
    test.assertAlmostEqual(row['decimal'], 2)
    test.assertEqual(row['int'], 2147483647)
    test.assertEqual(row['bigint'], -9223372036854775808)
    test.assertAlmostEqual(row['double'], 2e-308, delta=1e-311)
    test.assertEqual(row['date'], date(1970, 1, 1))
    test.assertEqual(
        row['timestamp'].to_pydatetime(),
        datetime(1970, 1, 2, 3, 4, 5, 600000),
    )
