/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.orc.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Unit test for the ORC BulkWriter write RowData with nested type. */
public class OrcBulkRowDataWriterTest {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @SuppressWarnings("FieldCanBeLocal")
    private String schema =
            "struct<_col0:string,_col1:int,_col2:array<struct<_col2_col0:string>>,"
                    + "_col3:map<string,struct<_col3_col0:string,_col3_col1:timestamp>>>";

    private LogicalType[] fieldTypes;
    private List<RowData> input;

    @Test
    public void testOrcBulkWriterWithRowData() throws Exception {
        final File outDir = TEMPORARY_FOLDER.newFolder();
        final Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", "LZ4");

        final OrcBulkWriterFactory<RowData> writer =
                new OrcBulkWriterFactory<>(
                        new RowDataVectorizer(schema, fieldTypes),
                        writerProps,
                        new Configuration());

        StreamingFileSink<RowData> sink =
                StreamingFileSink.forBulkFormat(new Path(outDir.toURI()), writer)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .withBucketCheckInterval(10000)
                        .build();

        try (OneInputStreamOperatorTestHarness<RowData, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)) {

            testHarness.setup();
            testHarness.open();

            int time = 0;
            for (final RowData record : input) {
                testHarness.processElement(record, ++time);
            }

            testHarness.snapshot(1, ++time);
            testHarness.notifyOfCompletedCheckpoint(1);

            validate(outDir, input);
        }
    }

    @Before
    public void initInput() {
        input = new ArrayList<>();
        fieldTypes = new LogicalType[4];
        fieldTypes[0] = new VarCharType();
        fieldTypes[1] = new IntType();
        List<RowType.RowField> arrayRowFieldList =
                Collections.singletonList(new RowType.RowField("_col2_col0", new VarCharType()));
        fieldTypes[2] = new ArrayType(new RowType(arrayRowFieldList));
        List<RowType.RowField> mapRowFieldList =
                Arrays.asList(
                        new RowType.RowField("_col3_col0", new VarCharType()),
                        new RowType.RowField("_col3_col1", new TimestampType()));
        fieldTypes[3] = new MapType(new VarCharType(), new RowType(mapRowFieldList));

        {
            GenericRowData rowData = new GenericRowData(4);

            rowData.setField(0, new BinaryStringData("_col_0_string_1"));

            rowData.setField(1, 1);

            GenericRowData arrayValue1 = new GenericRowData(1);
            arrayValue1.setField(0, new BinaryStringData("_col_2_row_0_string_1"));
            GenericRowData arrayValue2 = new GenericRowData(1);
            arrayValue2.setField(0, new BinaryStringData("_col_2_row_1_string_1"));
            GenericArrayData arrayData =
                    new GenericArrayData(new Object[] {arrayValue1, arrayValue2});
            rowData.setField(2, arrayData);

            GenericRowData mapValue1 = new GenericRowData(2);
            mapValue1.setField(0, new BinaryStringData(("_col_3_map_value_string_1")));
            mapValue1.setField(1, TimestampData.fromTimestamp(new Timestamp(3600000)));
            Map<StringData, RowData> mapDataMap = new HashMap<>();
            mapDataMap.put(new BinaryStringData("_col_3_map_key_1"), mapValue1);
            GenericMapData mapData = new GenericMapData(mapDataMap);
            rowData.setField(3, mapData);

            input.add(rowData);
        }

        {
            GenericRowData rowData = new GenericRowData(4);

            rowData.setField(0, new BinaryStringData("_col_0_string_2"));

            rowData.setField(1, 2);

            GenericRowData arrayValue1 = new GenericRowData(1);
            arrayValue1.setField(0, new BinaryStringData("_col_2_row_0_string_2"));
            GenericRowData arrayValue2 = new GenericRowData(1);
            arrayValue2.setField(0, new BinaryStringData("_col_2_row_1_string_2"));
            GenericArrayData arrayData =
                    new GenericArrayData(new Object[] {arrayValue1, arrayValue2});
            rowData.setField(2, arrayData);

            GenericRowData mapValue1 = new GenericRowData(2);
            mapValue1.setField(0, new BinaryStringData(("_col_3_map_value_string_2")));
            mapValue1.setField(1, TimestampData.fromTimestamp(new Timestamp(3600000)));
            Map<StringData, RowData> mapDataMap = new HashMap<>();
            mapDataMap.put(new BinaryStringData("_col_3_map_key_2"), mapValue1);
            GenericMapData mapData = new GenericMapData(mapDataMap);
            rowData.setField(3, mapData);
            input.add(rowData);
        }
    }

    private void validate(File files, List<RowData> expected) throws IOException {
        final File[] buckets = files.listFiles();
        assertNotNull(buckets);
        assertEquals(1, buckets.length);

        final File[] partFiles = buckets[0].listFiles();
        assertNotNull(partFiles);

        for (File partFile : partFiles) {
            assertTrue(partFile.length() > 0);

            OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());
            Reader reader =
                    OrcFile.createReader(
                            new org.apache.hadoop.fs.Path(partFile.toURI()), readerOptions);

            assertEquals(2, reader.getNumberOfRows());
            assertEquals(4, reader.getSchema().getFieldNames().size());
            assertSame(reader.getCompressionKind(), CompressionKind.LZ4);

            List<RowData> results = getResults(reader);

            assertEquals(2, results.size());
            assertEquals(results, expected);
        }
    }

    private static List<RowData> getResults(Reader reader) throws IOException {
        List<RowData> results = new ArrayList<>();

        RecordReader recordReader = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        while (recordReader.nextBatch(batch)) {
            BytesColumnVector stringVector = (BytesColumnVector) batch.cols[0];
            LongColumnVector intVector = (LongColumnVector) batch.cols[1];
            ListColumnVector listVector = (ListColumnVector) batch.cols[2];
            MapColumnVector mapVector = (MapColumnVector) batch.cols[3];

            for (int r = 0; r < batch.size; r++) {
                GenericRowData readRowData = new GenericRowData(4);

                readRowData.setField(0, readStringData(stringVector, r));
                readRowData.setField(1, readInt(intVector, r));
                readRowData.setField(2, readList(listVector, r));
                readRowData.setField(3, readMap(mapVector, r));

                results.add(readRowData);
            }
            recordReader.close();
        }

        return results;
    }

    private static StringData readStringData(BytesColumnVector stringVector, int row) {
        return new BinaryStringData(
                new String(
                        stringVector.vector[row],
                        stringVector.start[row],
                        stringVector.length[row]));
    }

    private static int readInt(LongColumnVector intVector, int row) {
        return (int) intVector.vector[row];
    }

    /** Read ListColumnVector with specify schema {@literal array<struct<_col2_col0:string>>}. */
    private static ArrayData readList(ListColumnVector listVector, int row) {
        int offset = (int) listVector.offsets[row];
        StructColumnVector structChild = (StructColumnVector) listVector.child;
        BytesColumnVector valueChild = (BytesColumnVector) structChild.fields[0];

        StringData value1 = readStringData(valueChild, offset);
        GenericRowData arrayValue1 = new GenericRowData(1);
        arrayValue1.setField(0, value1);

        StringData value2 = readStringData(valueChild, offset + 1);
        GenericRowData arrayValue2 = new GenericRowData(1);
        arrayValue2.setField(0, (value2));

        return new GenericArrayData(new Object[] {arrayValue1, arrayValue2});
    }

    /**
     * Read MapColumnVector with specify schema {@literal
     * map<string,struct<_col3_col0:string,_col3_col1:timestamp>>}.
     */
    private static MapData readMap(MapColumnVector mapVector, int row) {
        int offset = (int) mapVector.offsets[row];
        StringData keyData = readStringData((BytesColumnVector) mapVector.keys, offset);
        GenericRowData valueData = new GenericRowData(2);
        StructColumnVector structVector = (StructColumnVector) mapVector.values;
        BytesColumnVector bytesVector = (BytesColumnVector) structVector.fields[0];
        TimestampColumnVector timestampVector = (TimestampColumnVector) structVector.fields[1];
        StringData strValueData = readStringData(bytesVector, offset);
        TimestampData timestampData = readTimestamp(timestampVector, offset);
        valueData.setField(0, strValueData);
        valueData.setField(1, timestampData);
        Map<StringData, RowData> mapDataMap = new HashMap<>();
        mapDataMap.put(keyData, valueData);
        return new GenericMapData(mapDataMap);
    }

    private static TimestampData readTimestamp(TimestampColumnVector timestampVector, int row) {
        return TimestampData.fromTimestamp(timestampVector.asScratchTimestamp(row));
    }
}
