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
import glob
import os
import tempfile
import time
import unittest
from decimal import Decimal
from typing import List, Tuple

import pandas as pd
import pytz
from pyflink.common.time import Instant

from pyflink.common import Configuration, Row
from pyflink.common.typeinfo import RowTypeInfo, Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.tests.test_avro import \
    _create_basic_avro_schema_and_py_objects, _check_basic_avro_schema_results, \
    _create_enum_avro_schema_and_py_objects, _check_enum_avro_schema_results, \
    _create_union_avro_schema_and_py_objects, _check_union_avro_schema_results, \
    _create_array_avro_schema_and_py_objects, _check_array_avro_schema_results, \
    _create_map_avro_schema_and_py_objects, _check_map_avro_schema_results, \
    _create_map_avro_schema_and_records, _create_array_avro_schema_and_records, \
    _create_union_avro_schema_and_records, _create_enum_avro_schema_and_records, \
    _create_basic_avro_schema_and_records, _import_avro_classes
from pyflink.datastream.formats.avro import GenericRecordAvroTypeInfo, AvroSchema
from pyflink.datastream.formats.parquet import AvroParquetReaders, ParquetColumnarRowInputFormat, \
    AvroParquetWriters, ParquetBulkWriters
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.datastream.utils import create_hadoop_configuration
from pyflink.java_gateway import get_gateway
from pyflink.table.types import RowType, DataTypes, _to_java_data_type
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase, to_java_data_structure


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSourceAvroParquetReadersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        _import_avro_classes()

    def test_parquet_avro_basic(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_basic_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_basic")
        results = self.test_sink.get_results(True, False)
        _check_basic_avro_schema_results(self, results)

    def test_parquet_avro_enum(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_enum_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_enum")
        results = self.test_sink.get_results(True, False)
        _check_enum_avro_schema_results(self, results)

    def test_parquet_avro_union(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_union_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_union")
        results = self.test_sink.get_results(True, False)
        _check_union_avro_schema_results(self, results)

    def test_parquet_avro_array(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_array_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_array")
        results = self.test_sink.get_results(True, False)
        _check_array_avro_schema_results(self, results)

    def test_parquet_avro_map(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_map_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_map")
        results = self.test_sink.get_results(True, False)
        _check_map_avro_schema_results(self, results)

    def _build_parquet_avro_job(self, record_schema, *parquet_file_name):
        ds = self.env.from_source(
            FileSource.for_record_stream_format(
                AvroParquetReaders.for_generic_record(record_schema),
                *parquet_file_name
            ).build(),
            WatermarkStrategy.for_monotonous_timestamps(),
            "parquet-source"
        )
        ds.map(lambda e: e).add_sink(self.test_sink)

    @staticmethod
    def _create_parquet_avro_file(file_path: str, schema: AvroSchema, records: list):
        jvm = get_gateway().jvm
        j_path = jvm.org.apache.flink.core.fs.Path(file_path)
        writer = jvm.org.apache.flink.formats.parquet.avro.AvroParquetWriters \
            .forGenericRecord(schema._j_schema) \
            .create(j_path.getFileSystem().create(
                j_path,
                jvm.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
            ))
        for record in records:
            writer.addElement(record)
        writer.flush()
        writer.finish()


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSinkAvroParquetWritersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        # NOTE: parallelism == 1 is required to keep the order of results
        self.env.set_parallelism(1)
        self.parquet_dir_name = tempfile.mkdtemp(dir=self.tempdir)
        self.test_sink = DataStreamTestSinkFunction()

    def test_parquet_avro_basic_write(self):
        schema, objects = _create_basic_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_basic_write')
        results = self._read_parquet_avro_file(schema)
        _check_basic_avro_schema_results(self, results)

    def test_parquet_avro_enum_write(self):
        schema, objects = _create_enum_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_enum_write')
        results = self._read_parquet_avro_file(schema)
        _check_enum_avro_schema_results(self, results)

    def test_parquet_avro_union_write(self):
        schema, objects = _create_union_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_union_write')
        results = self._read_parquet_avro_file(schema)
        _check_union_avro_schema_results(self, results)

    def test_parquet_avro_array_write(self):
        schema, objects = _create_array_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_array_write')
        results = self._read_parquet_avro_file(schema)
        _check_array_avro_schema_results(self, results)

    def test_parquet_avro_map_write(self):
        schema, objects = _create_map_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_map_write')
        results = self._read_parquet_avro_file(schema)
        _check_map_avro_schema_results(self, results)

    def _build_avro_parquet_job(self, schema, objects):
        ds = self.env.from_collection(objects)
        avro_type_info = GenericRecordAvroTypeInfo(schema)
        sink = FileSink.for_bulk_format(
            self.parquet_dir_name, AvroParquetWriters.for_generic_record(schema)
        ).build()
        ds.map(lambda e: e, output_type=avro_type_info).sink_to(sink)

    def _read_parquet_avro_file(self, schema) -> List[dict]:
        parquet_files = [f for f in glob.glob(self.parquet_dir_name, recursive=True)]
        FileSourceAvroParquetReadersTests._build_parquet_avro_job(self, schema, *parquet_files)
        self.env.execute()
        return self.test_sink.get_results(True, False)


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet Columnar format tests')
class FileSourceParquetColumnarRowInputFormatTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        self.parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)

    def test_parquet_columnar_basic_read(self):
        os.environ['TZ'] = 'Asia/Shanghai'
        time.tzset()
        row_type, _, data = _create_parquet_basic_row_and_data()
        _write_row_data_to_parquet_file(self.parquet_file_name, row_type, data)
        self._build_parquet_columnar_job(row_type)
        self.env.execute('test_parquet_columnar_basic_read')
        results = self.test_sink.get_results(True, False)
        _check_parquet_basic_results(self, results)

    def _build_parquet_columnar_job(self, row_type: RowType):
        source = FileSource.for_bulk_file_format(
            ParquetColumnarRowInputFormat(row_type, Configuration(), 10, True, False),
            self.parquet_file_name
        ).build()
        ds = self.env.from_source(source, WatermarkStrategy.no_watermarks(), 'parquet-source')
        ds.map(lambda e: e).add_sink(self.test_sink)


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet RowData format tests')
class FileSinkParquetBulkWriterTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        # NOTE: parallelism == 1 is required to keep the order of results
        self.env.set_parallelism(1)
        self.parquet_dir_name = tempfile.mkdtemp(dir=self.tempdir)

    def test_parquet_row_data_basic_write(self):
        os.environ['TZ'] = 'Asia/Shanghai'
        time.tzset()
        row_type, row_type_info, data = _create_parquet_basic_row_and_data()
        self._build_parquet_job(row_type, row_type_info, data)
        self.env.execute('test_parquet_row_data_basic_write')
        results = self._read_parquet_file()
        _check_parquet_basic_results(self, results)

    def test_parquet_row_data_array_write(self):
        row_type, row_type_info, data = _create_parquet_array_row_and_data()
        self._build_parquet_job(row_type, row_type_info, data)
        self.env.execute('test_parquet_row_data_array_write')
        results = self._read_parquet_file()
        _check_parquet_array_results(self, results)

    @unittest.skip('ParquetSchemaConverter in flink-parquet annotate map keys as optional, but '
                   'Arrow restricts them to be required')
    def test_parquet_row_data_map_write(self):
        row_type, row_type_info, data = _create_parquet_map_row_and_data()
        self._build_parquet_job(row_type, row_type_info, data)
        self.env.execute('test_parquet_row_data_map_write')
        results = self._read_parquet_file()
        _check_parquet_map_results(self, results)

    def _build_parquet_job(self, row_type: RowType, row_type_info: RowTypeInfo, data: List[Row]):
        sink = FileSink.for_bulk_format(
            self.parquet_dir_name, ParquetBulkWriters.for_row_type(row_type, utc_timestamp=True)
        ).build()
        ds = self.env.from_collection(data, type_info=row_type_info)
        ds.sink_to(sink)

    def _read_parquet_file(self):
        records = []
        for file in glob.glob(os.path.join(os.path.join(self.parquet_dir_name, '**/*'))):
            df = pd.read_parquet(file)
            for i in range(df.shape[0]):
                records.append(df.loc[i])
        return records


def _write_row_data_to_parquet_file(path: str, row_type: RowType, rows: List[Row]):
    jvm = get_gateway().jvm
    flink = jvm.org.apache.flink

    j_output_stream = flink.core.fs.local.LocalDataOutputStream(jvm.java.io.File(path))
    j_bulk_writer = flink.formats.parquet.row.ParquetRowDataBuilder.createWriterFactory(
        _to_java_data_type(row_type).getLogicalType(),
        create_hadoop_configuration(Configuration()),
        True,
    ).create(j_output_stream)
    row_row_converter = flink.table.data.conversion.RowRowConverter.create(
        _to_java_data_type(row_type)
    )
    row_row_converter.open(row_row_converter.getClass().getClassLoader())
    for row in rows:
        j_bulk_writer.addElement(row_row_converter.toInternal(to_java_data_structure(row)))
    j_bulk_writer.finish()


def _create_parquet_basic_row_and_data() -> Tuple[RowType, RowTypeInfo, List[Row]]:
    row_type = DataTypes.ROW([
        DataTypes.FIELD('char', DataTypes.CHAR(10)),
        DataTypes.FIELD('varchar', DataTypes.VARCHAR(10)),
        DataTypes.FIELD('binary', DataTypes.BINARY(10)),
        DataTypes.FIELD('varbinary', DataTypes.VARBINARY(10)),
        DataTypes.FIELD('boolean', DataTypes.BOOLEAN()),
        DataTypes.FIELD('decimal', DataTypes.DECIMAL(2, 0)),
        DataTypes.FIELD('int', DataTypes.INT()),
        DataTypes.FIELD('bigint', DataTypes.BIGINT()),
        DataTypes.FIELD('double', DataTypes.DOUBLE()),
        DataTypes.FIELD('date', DataTypes.DATE().bridged_to('java.sql.Date')),
        DataTypes.FIELD('time', DataTypes.TIME().bridged_to('java.sql.Time')),
        DataTypes.FIELD('timestamp', DataTypes.TIMESTAMP(3).bridged_to('java.sql.Timestamp')),
        DataTypes.FIELD('timestamp_ltz', DataTypes.TIMESTAMP_LTZ(3)),
    ])
    row_type_info = Types.ROW_NAMED(
        ['char', 'varchar', 'binary', 'varbinary', 'boolean', 'decimal', 'int', 'bigint', 'double',
         'date', 'time', 'timestamp', 'timestamp_ltz'],
        [Types.STRING(), Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE()),
         Types.PRIMITIVE_ARRAY(Types.BYTE()), Types.BOOLEAN(), Types.BIG_DEC(), Types.INT(),
         Types.LONG(), Types.DOUBLE(), Types.SQL_DATE(), Types.SQL_TIME(), Types.SQL_TIMESTAMP(),
         Types.INSTANT()]
    )
    datetime_ltz = datetime.datetime(1970, 2, 3, 4, 5, 6, 700000, tzinfo=pytz.timezone('UTC'))
    timestamp_ltz = Instant.of_epoch_milli(
        (
            calendar.timegm(datetime_ltz.utctimetuple()) +
            calendar.timegm(time.localtime(0))
        ) * 1000 + datetime_ltz.microsecond // 1000
    )
    data = [Row(
        char='char',
        varchar='varchar',
        binary=b'binary',
        varbinary=b'varbinary',
        boolean=True,
        decimal=Decimal(1.5),
        int=2147483647,
        bigint=-9223372036854775808,
        double=2e-308,
        date=datetime.date(1970, 1, 1),
        time=datetime.time(1, 1, 1),
        timestamp=datetime.datetime(1970, 1, 2, 3, 4, 5, 600000),
        timestamp_ltz=timestamp_ltz
    )]
    return row_type, row_type_info, data


def _check_parquet_basic_results(test, results):
    row = results[0]
    test.assertEqual(row['char'], 'char')
    test.assertEqual(row['varchar'], 'varchar')
    test.assertEqual(row['binary'], b'binary')
    test.assertEqual(row['varbinary'], b'varbinary')
    test.assertEqual(row['boolean'], True)
    test.assertAlmostEqual(row['decimal'], 2)
    test.assertEqual(row['int'], 2147483647)
    test.assertEqual(row['bigint'], -9223372036854775808)
    test.assertAlmostEqual(row['double'], 2e-308, delta=1e-311)
    test.assertEqual(row['date'], datetime.date(1970, 1, 1))
    test.assertEqual(row['time'], datetime.time(1, 1, 1))
    ts = row['timestamp']
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    test.assertEqual(ts, datetime.datetime(1970, 1, 2, 3, 4, 5, 600000))
    ts_ltz = row['timestamp_ltz']
    if isinstance(ts_ltz, pd.Timestamp):
        ts_ltz = pytz.timezone('Asia/Shanghai').localize(ts_ltz.to_pydatetime())
    test.assertEqual(
        ts_ltz,
        pytz.timezone('Asia/Shanghai').localize(datetime.datetime(1970, 2, 3, 12, 5, 6, 700000))
    )


def _create_parquet_array_row_and_data() -> Tuple[RowType, RowTypeInfo, List[Row]]:
    row_type = DataTypes.ROW([
        DataTypes.FIELD(
            'string_array',
            DataTypes.ARRAY(DataTypes.STRING()).bridged_to('java.util.ArrayList')
        ),
        DataTypes.FIELD(
            'int_array',
            DataTypes.ARRAY(DataTypes.INT()).bridged_to('java.util.ArrayList')
        ),
    ])
    row_type_info = Types.ROW_NAMED([
        'string_array',
        'int_array',
    ], [
        Types.LIST(Types.STRING()),
        Types.LIST(Types.INT()),
    ])
    data = [Row(
        string_array=['a', 'b', 'c'],
        int_array=[1, 2, 3],
    )]
    return row_type, row_type_info, data


def _check_parquet_array_results(test, results):
    row = results[0]
    test.assertEqual(row['string_array'][0], 'a')
    test.assertEqual(row['string_array'][1], 'b')
    test.assertEqual(row['string_array'][2], 'c')
    test.assertEqual(row['int_array'][0], 1)
    test.assertEqual(row['int_array'][1], 2)
    test.assertEqual(row['int_array'][2], 3)


def _create_parquet_map_row_and_data() -> Tuple[RowType, RowTypeInfo, List[Row]]:
    row_type = DataTypes.ROW([
        DataTypes.FIELD('map', DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
    ])
    row_type_info = Types.ROW_NAMED(['map'], [Types.MAP(Types.INT(), Types.STRING())])
    data = [Row(
        map={0: 'a', 1: 'b', 2: 'c'}
    )]
    return row_type, row_type_info, data


def _check_parquet_map_results(test, results):
    m = {k: v for k, v in results[0]['map']}
    test.assertEqual(m[0], 'a')
    test.assertEqual(m[1], 'b')
    test.assertEqual(m[2], 'c')
