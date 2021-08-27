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

package org.apache.flink.table.runtime.batch;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed16;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/** Tests for interoperability with Avro types. */
public class AvroTypesITCase extends AbstractTestBase {

    private static final User USER_1 =
            User.newBuilder()
                    .setName("Charlie")
                    .setFavoriteColor("blue")
                    .setFavoriteNumber(null)
                    .setTypeBoolTest(false)
                    .setTypeDoubleTest(1.337d)
                    .setTypeNullTest(null)
                    .setTypeLongTest(1337L)
                    .setTypeArrayString(new ArrayList<>())
                    .setTypeArrayBoolean(new ArrayList<>())
                    .setTypeNullableArray(null)
                    .setTypeEnum(Colors.RED)
                    .setTypeMap(new HashMap<>())
                    .setTypeFixed(null)
                    .setTypeUnion(null)
                    .setTypeNested(
                            Address.newBuilder()
                                    .setNum(42)
                                    .setStreet("Bakerstreet")
                                    .setCity("Berlin")
                                    .setState("Berlin")
                                    .setZip("12049")
                                    .build())
                    .setTypeBytes(ByteBuffer.allocate(10))
                    .setTypeDate(LocalDate.parse("2014-03-01"))
                    .setTypeTimeMillis(LocalTime.parse("12:12:12"))
                    .setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"))
                    .setTypeTimestampMicros(
                            Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeDecimalBytes(
                            ByteBuffer.wrap(
                                    BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .setTypeDecimalFixed(
                            new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .build();

    private static final User USER_2 =
            User.newBuilder()
                    .setName("Whatever")
                    .setFavoriteNumber(null)
                    .setFavoriteColor("black")
                    .setTypeLongTest(42L)
                    .setTypeDoubleTest(0.0)
                    .setTypeNullTest(null)
                    .setTypeBoolTest(true)
                    .setTypeArrayString(Collections.singletonList("hello"))
                    .setTypeArrayBoolean(Collections.singletonList(true))
                    .setTypeEnum(Colors.GREEN)
                    .setTypeMap(new HashMap<>())
                    .setTypeFixed(new Fixed16())
                    .setTypeUnion(null)
                    .setTypeNested(null)
                    .setTypeDate(LocalDate.parse("2014-03-01"))
                    .setTypeBytes(ByteBuffer.allocate(10))
                    .setTypeTimeMillis(LocalTime.parse("12:12:12"))
                    .setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"))
                    .setTypeTimestampMicros(
                            Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeDecimalBytes(
                            ByteBuffer.wrap(
                                    BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .setTypeDecimalFixed(
                            new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .build();

    private static final User USER_3 =
            User.newBuilder()
                    .setName("Terminator")
                    .setFavoriteNumber(null)
                    .setFavoriteColor("yellow")
                    .setTypeLongTest(1L)
                    .setTypeDoubleTest(0.0)
                    .setTypeNullTest(null)
                    .setTypeBoolTest(false)
                    .setTypeArrayString(Collections.singletonList("world"))
                    .setTypeArrayBoolean(Collections.singletonList(false))
                    .setTypeEnum(Colors.GREEN)
                    .setTypeMap(new HashMap<>())
                    .setTypeFixed(new Fixed16())
                    .setTypeUnion(null)
                    .setTypeNested(null)
                    .setTypeBytes(ByteBuffer.allocate(10))
                    .setTypeDate(LocalDate.parse("2014-03-01"))
                    .setTypeTimeMillis(LocalTime.parse("12:12:12"))
                    .setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"))
                    .setTypeTimestampMicros(
                            Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS))
                    .setTypeDecimalBytes(
                            ByteBuffer.wrap(
                                    BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .setTypeDecimalFixed(
                            new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                    .build();

    @Test
    public void testAvroToRow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<User> ds = testData(env);
        Table t = tEnv.fromDataStream(ds, selectFields(ds));
        Table result = t.select($("*"));

        List<Row> results =
                CollectionUtil.iteratorToList(
                        DataStreamUtils.collect(tEnv.toAppendStream(result, Row.class)));
        // TODO we should get an Avro record here instead of a nested row.
        //  This should be fixed with FLIP-136
        String expected =
                "+I[black, null, Whatever, [true], [hello], true, java.nio.HeapByteBuffer[pos=0 lim=10 cap=10], "
                        + "2014-03-01, java.nio.HeapByteBuffer[pos=0 lim=2 cap=2], [7, -48], 0.0, GREEN, "
                        + "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 42, {}, null, null, null, 00:00:00.123456, "
                        + "12:12:12, 1970-01-01T00:00:00.123456Z, 2014-03-01T12:12:12.321Z, null]\n"
                        + "+I[blue, null, Charlie, [], [], false, java.nio.HeapByteBuffer[pos=0 lim=10 cap=10], 2014-03-01, "
                        + "java.nio.HeapByteBuffer[pos=0 lim=2 cap=2], [7, -48], 1.337, RED, null, 1337, {}, "
                        + "+I[Berlin, 42, Berlin, Bakerstreet, 12049], null, null, 00:00:00.123456, 12:12:12, 1970-01-01T00:00:00.123456Z, "
                        + "2014-03-01T12:12:12.321Z, null]\n"
                        + "+I[yellow, null, Terminator, [false], [world], false, "
                        + "java.nio.HeapByteBuffer[pos=0 lim=10 cap=10], 2014-03-01, "
                        + "java.nio.HeapByteBuffer[pos=0 lim=2 cap=2], [7, -48], 0.0, GREEN, "
                        + "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 1, {}, null, null, null, 00:00:00.123456, "
                        + "12:12:12, 1970-01-01T00:00:00.123456Z, 2014-03-01T12:12:12.321Z, null]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testAvroStringAccess() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<User> ds = testData(env);
        Table t = tEnv.fromDataStream(ds, selectFields(ds));
        Table result = t.select($("name"));
        List<Utf8> results =
                CollectionUtil.iteratorToList(result.execute().collect()).stream()
                        .map(row -> (Utf8) row.getField(0))
                        .collect(Collectors.toList());

        String expected = "Charlie\n" + "Terminator\n" + "Whatever";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testAvroObjectAccess() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<User> ds = testData(env);
        Table t = tEnv.fromDataStream(ds, selectFields(ds));
        Table result =
                t.filter($("type_nested").isNotNull())
                        .select($("type_nested").flatten())
                        .as("city", "num", "state", "street", "zip");

        List<Address> results =
                CollectionUtil.iteratorToList(
                        DataStreamUtils.collect(tEnv.toAppendStream(result, Address.class)));
        String expected = USER_1.getTypeNested().toString();
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testAvroToAvro() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<User> ds = testData(env);
        Table t = tEnv.fromDataStream(ds, selectFields(ds));
        Table result = t.select($("*"));

        List<User> results =
                CollectionUtil.iteratorToList(
                        DataStreamUtils.collect(tEnv.toAppendStream(result, User.class)));
        List<User> expected = Arrays.asList(USER_1, USER_2, USER_3);
        assertEquals(expected, results);
    }

    private DataStream<User> testData(StreamExecutionEnvironment env) {
        return env.fromElements(USER_1, USER_2, USER_3);
    }

    private static <T> Expression[] selectFields(DataStream<T> ds) {
        return Arrays.stream(((PojoTypeInfo<T>) ds.getType()).getFieldNames())
                .map(Expressions::$)
                .toArray(Expression[]::new);
    }
}
