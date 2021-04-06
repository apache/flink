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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.formats.utils.DeserializationSchemaMatcher.whenDeserializedWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/** Tests for the {@link JsonRowDeserializationSchema}. */
public class JsonRowDeserializationSchemaTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    /** Tests simple deserialization using type information. */
    @Test
    public void testTypeInfoDeserialization() throws Exception {
        long id = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        Timestamp timestamp = Timestamp.valueOf("1990-10-14 12:12:43");
        Date date = Date.valueOf("1990-10-14");
        Time time = Time.valueOf("12:12:43");

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", id);
        root.put("name", name);
        root.put("bytes", bytes);
        root.put("date1", "1990-10-14");
        root.put("date2", "1990-10-14");
        root.put("time1", "12:12:43Z");
        root.put("time2", "12:12:43Z");
        root.put("timestamp1", "1990-10-14T12:12:43Z");
        root.put("timestamp2", "1990-10-14T12:12:43Z");
        root.putObject("map").put("flink", 123);
        root.putObject("map2map").putObject("inner_map").put("key", 234);

        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        JsonRowDeserializationSchema deserializationSchema =
                new JsonRowDeserializationSchema.Builder(
                                Types.ROW_NAMED(
                                        new String[] {
                                            "id",
                                            "name",
                                            "bytes",
                                            "date1",
                                            "date2",
                                            "time1",
                                            "time2",
                                            "timestamp1",
                                            "timestamp2",
                                            "map",
                                            "map2map"
                                        },
                                        Types.LONG,
                                        Types.STRING,
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.SQL_DATE,
                                        Types.LOCAL_DATE,
                                        Types.SQL_TIME,
                                        Types.LOCAL_TIME,
                                        Types.SQL_TIMESTAMP,
                                        Types.LOCAL_DATE_TIME,
                                        Types.MAP(Types.STRING, Types.LONG),
                                        Types.MAP(
                                                Types.STRING, Types.MAP(Types.STRING, Types.INT))))
                        .build();

        Row row = new Row(11);
        row.setField(0, id);
        row.setField(1, name);
        row.setField(2, bytes);
        row.setField(3, date);
        row.setField(4, date.toLocalDate());
        row.setField(5, time);
        row.setField(6, time.toLocalTime());
        row.setField(7, timestamp);
        row.setField(8, timestamp.toLocalDateTime());
        row.setField(9, map);
        row.setField(10, nestedMap);

        assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(row));
    }

    @Test
    public void testSchemaDeserialization() throws Exception {
        final BigDecimal id = BigDecimal.valueOf(1238123899121L);
        final String name = "asdlkjasjkdla998y1122";
        final byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        final BigDecimal[] numbers =
                new BigDecimal[] {
                    BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3)
                };
        final String[] strings = new String[] {"one", "two", "three"};

        final ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", id.longValue());
        root.putNull("idOrNull");
        root.put("name", name);
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43Z");
        root.put("timestamp", "1990-10-14T12:12:43Z");
        root.put("bytes", bytes);
        root.putArray("numbers").add(1).add(2).add(3);
        root.putArray("strings").add("one").add("two").add("three");
        root.putObject("nested").put("booleanField", true).put("decimalField", 12);

        final byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        JsonRowDeserializationSchema deserializationSchema =
                new JsonRowDeserializationSchema.Builder(
                                "{"
                                        + "    type: 'object',"
                                        + "    properties: {"
                                        + "         id: { type: 'integer' },"
                                        + "         idOrNull: { type: ['integer', 'null'] },"
                                        + "         name: { type: 'string' },"
                                        + "         date: { type: 'string', format: 'date' },"
                                        + "         time: { type: 'string', format: 'time' },"
                                        + "         timestamp: { type: 'string', format: 'date-time' },"
                                        + "         bytes: { type: 'string', contentEncoding: 'base64' },"
                                        + "         numbers: { type: 'array', items: { type: 'integer' } },"
                                        + "         strings: { type: 'array', items: { type: 'string' } },"
                                        + "         nested: { "
                                        + "             type: 'object',"
                                        + "             properties: { "
                                        + "                 booleanField: { type: 'boolean' },"
                                        + "                 decimalField: { type: 'number' }"
                                        + "             }"
                                        + "         }"
                                        + "    }"
                                        + "}")
                        .build();

        final Row expected = new Row(10);
        expected.setField(0, id);
        expected.setField(1, null);
        expected.setField(2, name);
        expected.setField(3, Date.valueOf("1990-10-14"));
        expected.setField(4, Time.valueOf("12:12:43"));
        expected.setField(5, Timestamp.valueOf("1990-10-14 12:12:43"));
        expected.setField(6, bytes);
        expected.setField(7, numbers);
        expected.setField(8, strings);
        final Row nestedRow = new Row(2);
        nestedRow.setField(0, true);
        nestedRow.setField(1, BigDecimal.valueOf(12));
        expected.setField(9, nestedRow);

        assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(expected));
    }

    /** Tests deserialization with non-existing field name. */
    @Test
    public void testMissingNode() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", 123123123);
        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        TypeInformation<Row> rowTypeInformation =
                Types.ROW_NAMED(new String[] {"name"}, Types.STRING);

        JsonRowDeserializationSchema deserializationSchema =
                new JsonRowDeserializationSchema.Builder(rowTypeInformation).build();

        Row row = new Row(1);
        assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(row));

        deserializationSchema =
                new JsonRowDeserializationSchema.Builder(rowTypeInformation)
                        .failOnMissingField()
                        .build();

        assertThat(
                serializedJson,
                whenDeserializedWith(deserializationSchema)
                        .failsWithException(hasCause(instanceOf(IllegalStateException.class))));

        // ignore-parse-errors ignores missing field exception too
        deserializationSchema =
                new JsonRowDeserializationSchema.Builder(rowTypeInformation)
                        .ignoreParseErrors()
                        .build();
        assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(row));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "JSON format doesn't support failOnMissingField and ignoreParseErrors are both true");
        new JsonRowDeserializationSchema.Builder(rowTypeInformation)
                .failOnMissingField()
                .ignoreParseErrors()
                .build();
    }

    /** Tests that number of field names and types has to match. */
    @Test
    public void testNumberOfFieldNamesAndTypesMismatch() {
        try {
            new JsonRowDeserializationSchema.Builder(
                            Types.ROW_NAMED(new String[] {"one", "two", "three"}, Types.LONG))
                    .build();
            Assert.fail("Did not throw expected Exception");
        } catch (IllegalArgumentException ignored) {
            // Expected
        }
    }

    @Test
    public void testJsonParse() {
        for (TestSpec spec : testData) {
            testIgnoreParseErrors(spec);
            if (spec.errorMessage != null) {
                testParseErrors(spec);
            }
        }
    }

    private void testIgnoreParseErrors(TestSpec spec) {
        // the parsing field should be null and no exception is thrown
        JsonRowDeserializationSchema ignoreErrorsSchema =
                new JsonRowDeserializationSchema.Builder(spec.rowTypeInformation)
                        .ignoreParseErrors()
                        .build();
        Row expected;
        if (spec.expected != null) {
            expected = spec.expected;
        } else {
            expected = new Row(1);
        }
        assertThat(
                "Test Ignore Parse Error: " + spec.json,
                spec.json.getBytes(),
                whenDeserializedWith(ignoreErrorsSchema).equalsTo(expected));
    }

    private void testParseErrors(TestSpec spec) {
        // expect exception if parse error is not ignored
        JsonRowDeserializationSchema failingSchema =
                new JsonRowDeserializationSchema.Builder(spec.rowTypeInformation).build();
        assertThat(
                "Test Parse Error: " + spec.json,
                spec.json.getBytes(),
                whenDeserializedWith(failingSchema)
                        .failsWithException(hasMessage(containsString(spec.errorMessage))));
    }

    private static List<TestSpec> testData =
            Arrays.asList(
                    TestSpec.json("{\"id\": \"trueA\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.BOOLEAN))
                            .expect(Row.of(false)),
                    TestSpec.json("{\"id\": true}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.BOOLEAN))
                            .expect(Row.of(true)),
                    TestSpec.json("{\"id\":\"abc\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.INT))
                            .expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),
                    TestSpec.json("{\"id\":112.013}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.LONG))
                            .expect(Row.of(112L)),
                    TestSpec.json("{\"id\":true}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("true")),
                    TestSpec.json("{\"id\":123.234}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("123.234")),
                    TestSpec.json("{\"id\":1234567}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("1234567")),
                    TestSpec.json("{\"id\":\"string field\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("string field")),
                    TestSpec.json("{\"id\":[\"array data1\",\"array data2\",123,234.345]}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("[\"array data1\",\"array data2\",123,234.345]")),
                    TestSpec.json("{\"id\":{\"k1\":123,\"k2\":234.234,\"k3\":\"string data\"}}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.STRING))
                            .expect(Row.of("{\"k1\":123,\"k2\":234.234,\"k3\":\"string data\"}")),
                    TestSpec.json("{\"id\":\"long\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.LONG))
                            .expectErrorMessage("Failed to deserialize JSON '{\"id\":\"long\"}'"),
                    TestSpec.json("{\"id\":\"112.013.123\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.FLOAT))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),
                    TestSpec.json("{\"id\":\"112.013.123\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.DOUBLE))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),
                    TestSpec.json("{\"id\":\"18:00:243\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.SQL_TIME))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"id\":\"18:00:243\"}'"),
                    TestSpec.json("{\"id\":\"20191112\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.SQL_DATE))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"id\":\"20191112\"}'"),
                    TestSpec.json("{\"id\":\"2019-11-12 18:00:12\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.SQL_TIMESTAMP))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"id\":\"2019-11-12 18:00:12\"}'"),
                    TestSpec.json("{\"id\":\"abc\"}")
                            .typeInfo(Types.ROW_NAMED(new String[] {"id"}, Types.BIG_DEC))
                            .expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),
                    TestSpec.json("{\"row\":{\"id\":\"abc\"}}")
                            .typeInfo(
                                    Types.ROW_NAMED(
                                            new String[] {"row"},
                                            Types.ROW_NAMED(new String[] {"id"}, Types.INT)))
                            .expect(Row.of(new Row(1)))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"row\":{\"id\":\"abc\"}}'"),
                    TestSpec.json("{\"array\":[123, \"abc\"]}")
                            .typeInfo(
                                    Types.ROW_NAMED(
                                            new String[] {"array"}, Types.OBJECT_ARRAY(Types.INT)))
                            .expect(Row.of((Object) new Integer[] {123, null}))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"array\":[123, \"abc\"]}'"),
                    TestSpec.json("{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}")
                            .typeInfo(
                                    Types.ROW_NAMED(
                                            new String[] {"map"},
                                            Types.MAP(Types.STRING, Types.INT)))
                            .expect(Row.of(createHashMap("key1", 123, "key2", null)))
                            .expectErrorMessage(
                                    "Failed to deserialize JSON '{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}'"),
                    TestSpec.json("{\"id\":1,\"factor\":799.929496989092949698}")
                            .typeInfo(
                                    Types.ROW_NAMED(
                                            new String[] {"id", "factor"},
                                            Types.INT,
                                            Types.BIG_DEC))
                            .expect(Row.of(1, new BigDecimal("799.929496989092949698"))));

    private static Map<String, Integer> createHashMap(
            String k1, Integer v1, String k2, Integer v2) {
        Map<String, Integer> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static class TestSpec {
        private final String json;
        private @Nullable TypeInformation<Row> rowTypeInformation;
        private @Nullable Row expected;
        private @Nullable String errorMessage;

        private TestSpec(String json) {
            this.json = json;
        }

        public static TestSpec json(String json) {
            return new TestSpec(json);
        }

        TestSpec expect(Row row) {
            this.expected = row;
            return this;
        }

        TestSpec typeInfo(TypeInformation<Row> rowTypeInformation) {
            this.rowTypeInformation = rowTypeInformation;
            return this;
        }

        TestSpec expectErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
    }
}
