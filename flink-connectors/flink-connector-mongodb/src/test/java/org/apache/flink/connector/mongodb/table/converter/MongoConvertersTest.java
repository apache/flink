/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLoggerExtension;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Unit tests for {@link BsonToRowDataConverters} and {@link RowDataToBsonConverters}. */
@ExtendWith(TestLoggerExtension.class)
public class MongoConvertersTest {

    @Test
    public void testConvertBsonToRowData() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("_id", DataTypes.STRING()),
                        DataTypes.FIELD("f0", DataTypes.STRING()),
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.INT()),
                        DataTypes.FIELD("f3", DataTypes.BIGINT()),
                        DataTypes.FIELD("f4", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f5", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f6", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("f7", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("f8", DataTypes.STRING()),
                        DataTypes.FIELD("f9", DataTypes.STRING()),
                        DataTypes.FIELD("f10", DataTypes.STRING()),
                        DataTypes.FIELD("f11", DataTypes.STRING()),
                        DataTypes.FIELD("f12", DataTypes.STRING()),
                        DataTypes.FIELD("f13", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "f14", DataTypes.ROW(DataTypes.FIELD("f14_k", DataTypes.BIGINT()))),
                        DataTypes.FIELD("f15", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "f16",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f16_k", DataTypes.FLOAT())))),
                        DataTypes.FIELD("f17", DataTypes.STRING()));

        ObjectId oid = new ObjectId();
        UUID uuid = UUID.randomUUID();
        Instant now = Instant.now();

        BsonDocument docWithFullTypes =
                new BsonDocument()
                        .append("_id", new BsonObjectId(oid))
                        .append("f0", new BsonString("string"))
                        .append("f1", new BsonBinary(uuid))
                        .append("f2", new BsonInt32(2))
                        .append("f3", new BsonInt64(3L))
                        .append("f4", new BsonDouble(4.1d))
                        .append("f5", new BsonDecimal128(new Decimal128(new BigDecimal("5.1"))))
                        .append("f6", BsonBoolean.FALSE)
                        .append("f7", new BsonTimestamp((int) now.getEpochSecond(), 0))
                        .append("f8", new BsonDateTime(now.toEpochMilli()))
                        .append(
                                "f9",
                                new BsonRegularExpression(Pattern.compile("^9$").pattern(), "i"))
                        .append("f10", new BsonJavaScript("function() { return 10; }"))
                        .append(
                                "f11",
                                new BsonJavaScriptWithScope(
                                        "function() { return 11; }", new BsonDocument()))
                        .append("f12", new BsonSymbol("12"))
                        .append("f13", new BsonDbPointer("db.coll", oid))
                        .append("f14", new BsonDocument("f14_k", new BsonInt32(14)))
                        .append("f15", new BsonDocument("f15_k", new BsonInt32(15)))
                        .append(
                                "f16",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f16_k", new BsonDouble(16.1d)),
                                                new BsonDocument("f16_k", new BsonDouble(16.2d)))))
                        .append(
                                "f17",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f17_k", new BsonDouble(17.1d)),
                                                new BsonDocument("f17_k", new BsonDouble(17.2d)))));

        RowData expect =
                GenericRowData.of(
                        StringData.fromString(oid.toHexString()),
                        StringData.fromString("string"),
                        StringData.fromString(uuid.toString()),
                        2,
                        3L,
                        4.1d,
                        DecimalData.fromBigDecimal(new BigDecimal("5.1"), 10, 2),
                        false,
                        TimestampData.fromEpochMillis(now.getEpochSecond() * 1000),
                        StringData.fromString(
                                OffsetDateTime.ofInstant(now, ZoneOffset.UTC)
                                        .format(ISO_OFFSET_DATE_TIME)),
                        StringData.fromString("/^9$/i"),
                        StringData.fromString("function() { return 10; }"),
                        StringData.fromString("function() { return 11; }"),
                        StringData.fromString("12"),
                        StringData.fromString(oid.toHexString()),
                        GenericRowData.of(14L),
                        StringData.fromString("{\"f15_k\": 15}"),
                        new GenericArrayData(
                                new RowData[] {
                                    GenericRowData.of((float) 16.1d),
                                    GenericRowData.of((float) 16.2d)
                                }),
                        StringData.fromString("[{\"f17_k\": 17.1}, {\"f17_k\": 17.2}]"));

        // Test convert Bson to RowData
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(docWithFullTypes);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonNumberAndBooleanToSqlString() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.STRING()),
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.STRING()),
                        DataTypes.FIELD("f3", DataTypes.STRING()),
                        DataTypes.FIELD("f4", DataTypes.STRING()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))));

        RowData expect =
                GenericRowData.of(
                        StringData.fromString("false"),
                        StringData.fromString(String.valueOf(-1)),
                        StringData.fromString(String.valueOf(127L)),
                        StringData.fromString(String.valueOf(127.11d)),
                        StringData.fromString("127.11"));

        // Test for compatible bson number and boolean to string sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlBoolean() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("f1", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("f2", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("f3", DataTypes.BOOLEAN()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(1L))
                        .append("f3", new BsonString("true"));

        RowData expect = GenericRowData.of(false, false, true, true);

        // Test for compatible boolean sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlTinyInt() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.TINYINT()),
                        DataTypes.FIELD("f1", DataTypes.TINYINT()),
                        DataTypes.FIELD("f2", DataTypes.TINYINT()),
                        DataTypes.FIELD("f3", DataTypes.TINYINT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonString("127"));

        RowData expect = GenericRowData.of((byte) 0, (byte) -1, (byte) 127L, (byte) 127);

        // Test for compatible tinyint sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlSmallInt() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.SMALLINT()),
                        DataTypes.FIELD("f1", DataTypes.SMALLINT()),
                        DataTypes.FIELD("f2", DataTypes.SMALLINT()),
                        DataTypes.FIELD("f3", DataTypes.SMALLINT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonString("127"));

        RowData expect = GenericRowData.of((short) 0, (short) -1, (short) 127, (short) 127);

        // Test for compatible smallint sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlInt() {
        Instant now = Instant.now();

        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.INT()),
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.INT()),
                        DataTypes.FIELD("f3", DataTypes.INT()),
                        DataTypes.FIELD("f4", DataTypes.INT()),
                        DataTypes.FIELD("f5", DataTypes.INT()),
                        DataTypes.FIELD("f6", DataTypes.INT()),
                        DataTypes.FIELD("f7", DataTypes.INT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))))
                        .append("f5", new BsonString("127"))
                        .append("f6", new BsonDateTime(now.toEpochMilli()))
                        .append("f7", new BsonTimestamp((int) now.getEpochSecond(), 0));

        RowData expect =
                GenericRowData.of(
                        0,
                        -1,
                        127,
                        127,
                        127,
                        127,
                        (int) now.getEpochSecond(),
                        (int) now.getEpochSecond());

        // Test for compatible int sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlBigInt() {
        Instant now = Instant.now();

        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.BIGINT()),
                        DataTypes.FIELD("f1", DataTypes.BIGINT()),
                        DataTypes.FIELD("f2", DataTypes.BIGINT()),
                        DataTypes.FIELD("f3", DataTypes.BIGINT()),
                        DataTypes.FIELD("f4", DataTypes.BIGINT()),
                        DataTypes.FIELD("f5", DataTypes.BIGINT()),
                        DataTypes.FIELD("f6", DataTypes.BIGINT()),
                        DataTypes.FIELD("f7", DataTypes.BIGINT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))))
                        .append("f5", new BsonString("127"))
                        .append("f6", new BsonDateTime(now.toEpochMilli()))
                        .append("f7", new BsonTimestamp((int) now.getEpochSecond(), 0));

        RowData expect =
                GenericRowData.of(
                        0L,
                        -1L,
                        127L,
                        127L,
                        127L,
                        127L,
                        now.toEpochMilli(),
                        now.getEpochSecond() * 1000L);

        // Test for compatible int sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlDouble() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f1", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f2", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f3", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f4", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f5", DataTypes.DOUBLE()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))))
                        .append("f5", new BsonString("127.11"));

        RowData expect = GenericRowData.of(0d, -1d, 127d, 127.11d, 127.11d, 127.11d);

        // Test for compatible double sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlFloat() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.FLOAT()),
                        DataTypes.FIELD("f1", DataTypes.FLOAT()),
                        DataTypes.FIELD("f2", DataTypes.FLOAT()),
                        DataTypes.FIELD("f3", DataTypes.FLOAT()),
                        DataTypes.FIELD("f4", DataTypes.FLOAT()),
                        DataTypes.FIELD("f5", DataTypes.FLOAT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(-1))
                        .append("f2", new BsonInt64(127L))
                        .append("f3", new BsonDouble(127.11d))
                        .append("f4", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))))
                        .append("f5", new BsonString("127.11"));

        RowData expect = GenericRowData.of(0f, -1f, 127f, 127.11f, 127.11f, 127.11f);

        // Test for compatible float sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonToSqlDecimal() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f1", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f2", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f3", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f4", DataTypes.DECIMAL(10, 2)));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", new BsonInt32(-1))
                        .append("f1", new BsonInt64(127L))
                        .append("f2", new BsonDouble(127.11d))
                        .append("f3", new BsonDecimal128(new Decimal128(new BigDecimal("127.11"))))
                        .append("f4", new BsonString("127.11"));

        RowData expect =
                GenericRowData.of(
                        DecimalData.fromBigDecimal(new BigDecimal("-1"), 10, 2),
                        DecimalData.fromBigDecimal(new BigDecimal("127"), 10, 2),
                        DecimalData.fromBigDecimal(new BigDecimal("127.11"), 10, 2),
                        DecimalData.fromBigDecimal(new BigDecimal("127.11"), 10, 2),
                        DecimalData.fromBigDecimal(new BigDecimal("127.11"), 10, 2));

        // Test for compatible float sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertBsonInfiniteDecimalToSqlNumber() {
        BsonDecimal128 positiveInfinity = new BsonDecimal128(Decimal128.POSITIVE_INFINITY);
        BsonDecimal128 negativeInfinity = new BsonDecimal128(Decimal128.NEGATIVE_INFINITY);

        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.INT()),
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.BIGINT()),
                        DataTypes.FIELD("f3", DataTypes.BIGINT()),
                        DataTypes.FIELD("f4", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f5", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f6", DataTypes.FLOAT()),
                        DataTypes.FIELD("f7", DataTypes.FLOAT()));

        BsonDocument document =
                new BsonDocument()
                        .append("f0", positiveInfinity)
                        .append("f1", negativeInfinity)
                        .append("f2", positiveInfinity)
                        .append("f3", negativeInfinity)
                        .append("f4", positiveInfinity)
                        .append("f5", negativeInfinity)
                        .append("f6", positiveInfinity)
                        .append("f7", negativeInfinity);

        RowData expect =
                GenericRowData.of(
                        Integer.MAX_VALUE,
                        Integer.MIN_VALUE,
                        Long.MAX_VALUE,
                        Long.MIN_VALUE,
                        Double.MAX_VALUE,
                        -Double.MAX_VALUE,
                        Float.MAX_VALUE,
                        -Float.MAX_VALUE);

        // Test for compatible decimal sql type conversions
        BsonToRowDataConverters.BsonToRowDataConverter bsonToRowDataConverter =
                BsonToRowDataConverters.createNullableConverter(rowType.getLogicalType());
        RowData actual = (RowData) bsonToRowDataConverter.convert(document);
        assertThat(actual, equalTo(expect));
    }

    @Test
    public void testConvertRowDataToBson() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("_id", DataTypes.STRING()),
                        DataTypes.FIELD("f0", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("f1", DataTypes.TINYINT()),
                        DataTypes.FIELD("f2", DataTypes.SMALLINT()),
                        DataTypes.FIELD("f3", DataTypes.INT()),
                        DataTypes.FIELD("f4", DataTypes.BIGINT()),
                        DataTypes.FIELD("f5", DataTypes.FLOAT()),
                        DataTypes.FIELD("f6", DataTypes.DOUBLE()),
                        DataTypes.FIELD("f7", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("f8", DataTypes.TIMESTAMP_LTZ(6)),
                        DataTypes.FIELD(
                                "f9", DataTypes.ROW(DataTypes.FIELD("f9_k", DataTypes.BIGINT()))),
                        DataTypes.FIELD(
                                "f10",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f10_k", DataTypes.FLOAT())))));

        ObjectId oid = new ObjectId();
        Instant now = Instant.now();

        RowData rowData =
                GenericRowData.of(
                        StringData.fromString(oid.toHexString()),
                        false,
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        5.1f,
                        6.1d,
                        DecimalData.fromBigDecimal(new BigDecimal("7.1"), 10, 2),
                        TimestampData.fromEpochMillis(now.toEpochMilli()),
                        GenericRowData.of(9L),
                        new GenericArrayData(
                                new RowData[] {
                                    GenericRowData.of(10.1f), GenericRowData.of(10.2f)
                                }));

        BsonDocument expect =
                new BsonDocument()
                        .append("_id", new BsonString(oid.toHexString()))
                        .append("f0", BsonBoolean.FALSE)
                        .append("f1", new BsonInt32(1))
                        .append("f2", new BsonInt32(2))
                        .append("f3", new BsonInt32(3))
                        .append("f4", new BsonInt64(4))
                        .append("f5", new BsonDouble(5.1f))
                        .append("f6", new BsonDouble(6.1d))
                        .append("f7", new BsonDecimal128(new Decimal128(new BigDecimal("7.10"))))
                        .append("f8", new BsonDateTime(now.toEpochMilli()))
                        .append("f9", new BsonDocument("f9_k", new BsonInt64(9L)))
                        .append(
                                "f10",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonDocument("f10_k", new BsonDouble(10.1f)),
                                                new BsonDocument("f10_k", new BsonDouble(10.2f)))));

        // Test convert RowData to Bson
        RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter =
                RowDataToBsonConverters.createNullableConverter(rowType.getLogicalType());
        BsonDocument actual = (BsonDocument) rowDataToBsonConverter.convert(rowData);
        assertThat(actual, equalTo(expect));
    }
}
