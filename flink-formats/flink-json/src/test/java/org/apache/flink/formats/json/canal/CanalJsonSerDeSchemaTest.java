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

package org.apache.flink.formats.json.canal;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link CanalJsonSerializationSchema} and {@link CanalJsonDeserializationSchema}. */
public class CanalJsonSerDeSchemaTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private static final DataType PHYSICAL_DATA_TYPE =
            ROW(
                    FIELD("id", INT().notNull()),
                    FIELD("name", STRING()),
                    FIELD("description", STRING()),
                    FIELD("weight", FLOAT()));

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = readLines("canal-data-filter-table.txt");
        CanalJsonDeserializationSchema deserializationSchema =
                CanalJsonDeserializationSchema.builder(
                                PHYSICAL_DATA_TYPE,
                                Collections.emptyList(),
                                InternalTypeInfo.of(PHYSICAL_DATA_TYPE.getLogicalType()))
                        .setDatabase("^my.*")
                        .setTable("^prod.*")
                        .build();
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeNullRow() throws Exception {
        final List<ReadableMetadata> requestedMetadata = Arrays.asList(ReadableMetadata.values());
        final CanalJsonDeserializationSchema deserializationSchema =
                createCanalJsonDeserializationSchema(null, null, requestedMetadata);
        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[0], collector);
        assertEquals(0, collector.list.size());
    }

    @Test
    public void testDeserializationWithMetadata() throws Exception {
        testDeserializationWithMetadata(
                "canal-data.txt",
                null,
                null,
                row -> {
                    assertThat(row.getInt(0), equalTo(101));
                    assertThat(row.getString(1).toString(), equalTo("scooter"));
                    assertThat(row.getString(2).toString(), equalTo("Small 2-wheel scooter"));
                    assertThat(row.getFloat(3), equalTo(3.14f));
                    assertThat(row.getString(4).toString(), equalTo("inventory"));
                    assertThat(row.getString(5).toString(), equalTo("products2"));
                    assertThat(row.getMap(6).size(), equalTo(4));
                    assertThat(row.getArray(7).getString(0).toString(), equalTo("id"));
                    assertThat(row.getTimestamp(8, 3).getMillisecond(), equalTo(1589373515477L));
                    assertThat(row.getTimestamp(9, 3).getMillisecond(), equalTo(1589373515000L));
                });
        testDeserializationWithMetadata(
                "canal-data-filter-table.txt",
                "mydb",
                "product",
                row -> {
                    assertThat(row.getInt(0), equalTo(101));
                    assertThat(row.getString(1).toString(), equalTo("scooter"));
                    assertThat(row.getString(2).toString(), equalTo("Small 2-wheel scooter"));
                    assertThat(row.getFloat(3), equalTo(3.14f));
                    assertThat(row.getString(4).toString(), equalTo("mydb"));
                    assertThat(row.getString(5).toString(), equalTo("product"));
                    assertThat(row.getMap(6).size(), equalTo(4));
                    assertThat(row.getArray(7).getString(0).toString(), equalTo("id"));
                    assertThat(row.getTimestamp(8, 3).getMillisecond(), equalTo(1598944146308L));
                    assertThat(row.getTimestamp(9, 3).getMillisecond(), equalTo(1598944132000L));
                });
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        List<String> lines = readLines("canal-data.txt");
        CanalJsonDeserializationSchema deserializationSchema =
                CanalJsonDeserializationSchema.builder(
                                PHYSICAL_DATA_TYPE,
                                Collections.emptyList(),
                                InternalTypeInfo.of(PHYSICAL_DATA_TYPE.getLogicalType()))
                        .setIgnoreParseErrors(false)
                        .setTimestampFormat(TimestampFormat.ISO_8601)
                        .build();
        runTest(lines, deserializationSchema);
    }

    public void runTest(List<String> lines, CanalJsonDeserializationSchema deserializationSchema)
            throws Exception {
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        // Canal captures change data (`canal-data.txt`) on the `product` table:
        //
        // CREATE TABLE product (
        //  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
        //  name VARCHAR(255),
        //  description VARCHAR(512),
        //  weight FLOAT
        // );
        // ALTER TABLE product AUTO_INCREMENT = 101;
        //
        // INSERT INTO product
        // VALUES (default,"scooter","Small 2-wheel scooter",3.14),
        //        (default,"car battery","12V car battery",8.1),
        //        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40
        // to #3",0.8),
        //        (default,"hammer","12oz carpenter's hammer",0.75),
        //        (default,"hammer","14oz carpenter's hammer",0.875),
        //        (default,"hammer","16oz carpenter's hammer",1.0),
        //        (default,"rocks","box of assorted rocks",5.3),
        //        (default,"jacket","water resistent black wind breaker",0.1),
        //        (default,"spare tire","24 inch spare tire",22.2);
        // UPDATE product SET description='18oz carpenter hammer' WHERE id=106;
        // UPDATE product SET weight='5.1' WHERE id=107;
        // INSERT INTO product VALUES (default,"jacket","water resistent white wind breaker",0.2);
        // INSERT INTO product VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
        // UPDATE product SET description='new water resistent white wind breaker', weight='0.5'
        // WHERE id=110;
        // UPDATE product SET weight='5.17' WHERE id=111;
        // DELETE FROM product WHERE id=111;
        // UPDATE product SET weight='5.17' WHERE id=102 or id = 101;
        // DELETE FROM product WHERE id=102 or id = 103;
        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.14)",
                        "+I(102,car battery,12V car battery,8.1)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8)",
                        "+I(104,hammer,12oz carpenter's hammer,0.75)",
                        "+I(105,hammer,14oz carpenter's hammer,0.875)",
                        "+I(106,hammer,null,1.0)",
                        "+I(107,rocks,box of assorted rocks,5.3)",
                        "+I(108,jacket,water resistent black wind breaker,0.1)",
                        "+I(109,spare tire,24 inch spare tire,22.2)",
                        "-U(106,hammer,null,1.0)",
                        "+U(106,hammer,18oz carpenter hammer,1.0)",
                        "-U(107,rocks,box of assorted rocks,5.3)",
                        "+U(107,rocks,box of assorted rocks,5.1)",
                        "+I(110,jacket,water resistent white wind breaker,0.2)",
                        "+I(111,scooter,Big 2-wheel scooter ,5.18)",
                        "-U(110,jacket,water resistent white wind breaker,0.2)",
                        "+U(110,jacket,new water resistent white wind breaker,0.5)",
                        "-U(111,scooter,Big 2-wheel scooter ,5.18)",
                        "+U(111,scooter,Big 2-wheel scooter ,5.17)",
                        "-D(111,scooter,Big 2-wheel scooter ,5.17)",
                        "-U(101,scooter,Small 2-wheel scooter,3.14)",
                        "+U(101,scooter,Small 2-wheel scooter,5.17)",
                        "-U(102,car battery,12V car battery,8.1)",
                        "+U(102,car battery,12V car battery,5.17)",
                        "-D(102,car battery,12V car battery,5.17)",
                        "-D(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8)");
        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        // test Serialization
        CanalJsonSerializationSchema serializationSchema =
                new CanalJsonSerializationSchema(
                        (RowType) PHYSICAL_DATA_TYPE.getLogicalType(),
                        TimestampFormat.ISO_8601,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        true);
        serializationSchema.open(null);

        List<String> result = new ArrayList<>();
        for (RowData rowData : collector.list) {
            result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
        }

        List<String> expectedResult =
                Arrays.asList(
                        "{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":null,\"weight\":1.0}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":null,\"weight\":1.0}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":1.0}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.1}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":0.5}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":5.17}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17}],\"type\":\"DELETE\"}",
                        "{\"data\":[{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8}],\"type\":\"DELETE\"}");

        assertEquals(expectedResult, result);
    }

    private void testDeserializationWithMetadata(
            String resourceFile, String database, String table, Consumer<RowData> testConsumer)
            throws Exception {
        // we only read the first line for keeping the test simple
        final String firstLine = readLines(resourceFile).get(0);
        final List<ReadableMetadata> requestedMetadata = Arrays.asList(ReadableMetadata.values());
        final CanalJsonDeserializationSchema deserializationSchema =
                createCanalJsonDeserializationSchema(database, table, requestedMetadata);
        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(firstLine.getBytes(StandardCharsets.UTF_8), collector);
        assertEquals(9, collector.list.size());
        testConsumer.accept(collector.list.get(0));
    }

    private CanalJsonDeserializationSchema createCanalJsonDeserializationSchema(
            String database, String table, List<ReadableMetadata> requestedMetadata) {
        final DataType producedDataType =
                DataTypeUtils.appendRowFields(
                        PHYSICAL_DATA_TYPE,
                        requestedMetadata.stream()
                                .map(m -> DataTypes.FIELD(m.key, m.dataType))
                                .collect(Collectors.toList()));
        return CanalJsonDeserializationSchema.builder(
                        PHYSICAL_DATA_TYPE,
                        requestedMetadata,
                        InternalTypeInfo.of(producedDataType.getLogicalType()))
                .setDatabase(database)
                .setTable(table)
                .setIgnoreParseErrors(false)
                .setTimestampFormat(TimestampFormat.ISO_8601)
                .build();
    }

    @Test
    public void testParseNonNumericNumbers() throws Exception {
        List<String> lines =
                Arrays.asList(
                        "{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":NaN}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":102,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":Infinity}],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":103,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":-Infinity}],\"type\":\"INSERT\"}");

        CanalJsonDeserializationSchema deserializationSchema =
                CanalJsonDeserializationSchema.builder(
                                PHYSICAL_DATA_TYPE,
                                Collections.emptyList(),
                                InternalTypeInfo.of(PHYSICAL_DATA_TYPE.getLogicalType()))
                        .setAllowNonNumericNumbers(true)
                        .setTimestampFormat(TimestampFormat.ISO_8601)
                        .build();

        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,NaN)",
                        "+I(102,scooter,Small 2-wheel scooter,Infinity)",
                        "+I(103,scooter,Small 2-wheel scooter,-Infinity)");

        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());

        assertEquals(expected, actual);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static List<String> readLines(String resource) throws IOException {
        final URL url = CanalJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
