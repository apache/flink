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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ParquetRowDataBuilder} and {@link ParquetRowDataWriter}. */
class ParquetRowDataWriterTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarBinaryType(VarBinaryType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0));

    private static final RowType ROW_TYPE_COMPLEX =
            RowType.of(
                    new ArrayType(true, new IntType()),
                    new MapType(
                            true,
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)),
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

    @SuppressWarnings("unchecked")
    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER_COMPLEX =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ROW_TYPE_COMPLEX));

    @SuppressWarnings("unchecked")
    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ROW_TYPE));

    @Test
    void testTypes(@TempDir java.nio.file.Path folder) throws Exception {
        Configuration conf = new Configuration();
        innerTest(folder, conf, true);
        innerTest(folder, conf, false);
        complexTypeTest(folder, conf, true);
        complexTypeTest(folder, conf, false);
    }

    @Test
    void testCompression(@TempDir java.nio.file.Path folder) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ParquetOutputFormat.COMPRESSION, "GZIP");
        innerTest(folder, conf, true);
        innerTest(folder, conf, false);
        complexTypeTest(folder, conf, true);
        complexTypeTest(folder, conf, false);
    }

    private void innerTest(java.nio.file.Path folder, Configuration conf, boolean utcTimestamp)
            throws IOException {
        Path path = new Path(folder.toString(), UUID.randomUUID().toString());
        int number = 1000;
        List<Row> rows = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            rows.add(
                    Row.of(
                            String.valueOf(v),
                            String.valueOf(v).getBytes(StandardCharsets.UTF_8),
                            v % 2 == 0,
                            v.byteValue(),
                            v.shortValue(),
                            v,
                            v.longValue(),
                            v.floatValue(),
                            v.doubleValue(),
                            toDateTime(v),
                            BigDecimal.valueOf(v),
                            BigDecimal.valueOf(v),
                            BigDecimal.valueOf(v)));
        }

        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(ROW_TYPE, conf, utcTimestamp);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        for (int i = 0; i < number; i++) {
            writer.addElement(CONVERTER.toInternal(rows.get(i)));
        }
        writer.flush();
        writer.finish();

        // verify
        ParquetColumnarRowSplitReader reader =
                ParquetSplitReaderUtil.genPartColumnarRowReader(
                        utcTimestamp,
                        true,
                        conf,
                        ROW_TYPE.getFieldNames().toArray(new String[0]),
                        ROW_TYPE.getChildren().stream()
                                .map(TypeConversions::fromLogicalToDataType)
                                .toArray(DataType[]::new),
                        new HashMap<>(),
                        IntStream.range(0, ROW_TYPE.getFieldCount()).toArray(),
                        50,
                        path,
                        0,
                        Long.MAX_VALUE);
        int cnt = 0;
        while (!reader.reachedEnd()) {
            Row row = CONVERTER.toExternal(reader.nextRecord());
            assertThat(row).isEqualTo(rows.get(cnt));
            cnt++;
        }
        assertThat(cnt).isEqualTo(number);
    }

    public void complexTypeTest(java.nio.file.Path folder, Configuration conf, boolean utcTimestamp)
            throws Exception {
        Path path = new Path(folder.toString(), UUID.randomUUID().toString());
        int number = 1000;
        List<Row> rows = new ArrayList<>(number);
        Map<String, String> mapData = new HashMap<>();
        mapData.put("k1", "v1");
        mapData.put(null, "v2");
        mapData.put("k2", null);

        for (int i = 0; i < number; i++) {
            Integer v = i;
            rows.add(Row.of(new Integer[] {v}, mapData, Row.of(String.valueOf(v), v)));
        }

        ParquetWriterFactory<RowData> factory =
                ParquetRowDataBuilder.createWriterFactory(ROW_TYPE_COMPLEX, conf, utcTimestamp);
        BulkWriter<RowData> writer =
                factory.create(path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
        for (int i = 0; i < number; i++) {
            writer.addElement(CONVERTER_COMPLEX.toInternal(rows.get(i)));
        }
        writer.flush();
        writer.finish();

        File file = new File(path.getPath());
        final List<Row> fileContent = readParquetFile(file);
        assertThat(fileContent).isEqualTo(rows);
    }

    private static List<Row> readParquetFile(File file) throws IOException {
        InputFile inFile =
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(file.toURI()), new Configuration());

        ArrayList<Row> results = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(inFile).build()) {
            GenericRecord next;
            while ((next = reader.read()) != null) {
                Integer c0 = (Integer) ((ArrayList<GenericData.Record>) next.get(0)).get(0).get(0);
                HashMap<Utf8, Utf8> map = ((HashMap<Utf8, Utf8>) next.get(1));
                String c21 = ((GenericData.Record) next.get(2)).get(0).toString();
                Integer c22 = (Integer) ((GenericData.Record) next.get(2)).get(1);

                Map<String, String> c1 = new HashMap<>();
                for (Utf8 key : map.keySet()) {
                    String k = key == null ? null : key.toString();
                    String v = map.get(key) == null ? null : map.get(key).toString();
                    c1.put(k, v);
                }

                Row row = Row.of(new Integer[] {c0}, c1, Row.of(c21, c22));
                results.add(row);
            }
        }

        return results;
    }

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 1000;
        return LocalDateTime.now().plusNanos(v).plusSeconds(v);
    }
}
