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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.thrift.generated.Diagnostics;
import org.apache.flink.formats.thrift.generated.Event;
import org.apache.flink.formats.thrift.generated.TestData;
import org.apache.flink.formats.thrift.generated.UserAction;
import org.apache.flink.formats.thrift.generated.UserEventType;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.formats.thrift.generated.TestEnum.ENUM_TYPE1;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link ThriftRowDataSerializationSchema} and {@link ThriftRowDataDeserializationSchema}.
 */
@RunWith(Parameterized.class)
public class ThriftRowDataSerDeSchemaTest {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() throws TException {
        return Arrays.asList(
                // [0] test tcompact protocol
                TestSpec.thrift(
                                new Diagnostics()
                                        .setHostname("testHostName")
                                        .setIpaddress("testIPAddress"))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .expect(Row.of("testHostName", "testIPAddress"))
                        .rowType(ROW(FIELD("hostname", STRING()), FIELD("ipaddress", STRING()))),

                // [1] test binary protocol
                TestSpec.thrift(
                                new Diagnostics()
                                        .setHostname("testHostName")
                                        .setIpaddress("testIPAddress"))
                        .protocol("org.apache.thrift.protocol.TBinaryProtocol")
                        .expect(Row.of("testHostName", "testIPAddress"))
                        .rowType(ROW(FIELD("hostname", STRING()), FIELD("ipaddress", STRING()))),

                // [2] test json protocol
                TestSpec.thrift(
                                new Diagnostics()
                                        .setHostname("testHostName")
                                        .setIpaddress("testIPAddress"))
                        .protocol("org.apache.thrift.protocol.TJSONProtocol")
                        .expect(Row.of("testHostName", "testIPAddress"))
                        .rowType(ROW(FIELD("hostname", STRING()), FIELD("ipaddress", STRING()))),

                // [3] test ttuple protocol
                TestSpec.thrift(
                                new Diagnostics()
                                        .setHostname("testHostName")
                                        .setIpaddress("testIPAddress"))
                        .protocol("org.apache.thrift.protocol.TTupleProtocol")
                        .expect(Row.of("testHostName", "testIPAddress"))
                        .rowType(ROW(FIELD("hostname", STRING()), FIELD("ipaddress", STRING()))),

                // [4] test nested data
                TestSpec.thrift(
                                new Event()
                                        .setTimestamp(0xffffL)
                                        .setUserId(0xffffL)
                                        .setEventType(UserEventType.BROWSE)
                                        .setAuxData(
                                                new HashMap<String, String>() {
                                                    {
                                                        put("key1", "value1");
                                                        put("key2", "value2");
                                                        put("key3", "value3");
                                                    }
                                                })
                                        .setUserActions(
                                                new HashMap<String, UserAction>() {
                                                    {
                                                        put(
                                                                "action1",
                                                                new UserAction()
                                                                        .setReferralUrl(
                                                                                "testReferralUser")
                                                                        .setUrl("testUrl")
                                                                        .setTimestamp(0xffffL));
                                                    }
                                                })
                                        .setDiagnostics(
                                                new Diagnostics()
                                                        .setHostname("testHostName")
                                                        .setIpaddress("testIPAddress")))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .expect(
                                Row.of(
                                        0xffffL,
                                        0xffffL,
                                        UserEventType.BROWSE.name(),
                                        new HashMap<String, String>() {
                                            {
                                                put("key1", "value1");
                                                put("key2", "value2");
                                                put("key3", "value3");
                                            }
                                        },
                                        new HashMap<String, Row>() {
                                            {
                                                put(
                                                        "action1",
                                                        Row.of(
                                                                0xffffL,
                                                                "testUrl",
                                                                "testReferralUser"));
                                            }
                                        },
                                        Row.of("testHostName", "testIPAddress")))
                        .rowType(
                                ROW(
                                        FIELD("timestamp", BIGINT()),
                                        FIELD("userId", BIGINT()),
                                        FIELD("eventType", STRING()),
                                        FIELD("auxData", MAP(STRING(), STRING())),
                                        FIELD(
                                                "userActions",
                                                MAP(
                                                        STRING(),
                                                        ROW(
                                                                FIELD("timestamp", BIGINT()),
                                                                FIELD("url", STRING()),
                                                                FIELD("referralUrl", STRING())))),
                                        FIELD(
                                                "diagnostics",
                                                ROW(
                                                        FIELD("hostname", STRING()),
                                                        FIELD("ipaddress", STRING()))))),

                // [5] test all thrift basic type and container type with scrooge code generate
                TestSpec.thrift(
                                new TestData()
                                        .setTestBool(true)
                                        .setTestByte((byte) 0xff)
                                        .setTestShort((short) 0xffff)
                                        .setTestInt(0xffffffff)
                                        .setTestLong(0xffffffffffffffffL)
                                        .setTestString("Hello, Flink thrift format")
                                        .setTestBinary("Hello, Flink thrift format".getBytes())
                                        .setTestEnum(ENUM_TYPE1)
                                        .setTestMapString(
                                                new HashMap<String, String>() {
                                                    {
                                                        put("key1", "value1");
                                                        put("key2", "value2");
                                                    }
                                                })
                                        .setTestListString(Arrays.asList("value1", "value2")))
                        .expect(
                                Row.of(
                                        true,
                                        (byte) 0xff,
                                        (short) 0xffff,
                                        0xffffffff,
                                        0xffffffffffffffffL,
                                        "Hello, Flink thrift format",
                                        "Hello, Flink thrift format".getBytes(),
                                        ENUM_TYPE1.name(),
                                        new HashMap<String, String>() {
                                            {
                                                put("key1", "value1");
                                                put("key2", "value2");
                                            }
                                        },
                                        new String[] {"value1", "value2"}))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .rowType(
                                ROW(
                                        FIELD("testBool", BOOLEAN()),
                                        FIELD("testByte", TINYINT()),
                                        FIELD("testShort", SMALLINT()),
                                        FIELD("testInt", INT()),
                                        FIELD("testLong", BIGINT()),
                                        FIELD("testString", STRING()),
                                        FIELD("testBinary", VARBINARY(1024)),
                                        FIELD("testEnum", STRING()),
                                        FIELD("testMapString", MAP(STRING(), STRING())),
                                        FIELD("testListString", ARRAY(STRING())))),

                // [6] test binary field that generated by thrift
                TestSpec.thrift(
                                new TestData()
                                        .setTestBinary("Hello, Flink thrift format".getBytes()))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .rowType(ROW(FIELD("testBinary", VARBINARY(1024))))
                        .expect(Row.of("Hello, Flink thrift format".getBytes()))
                        .allMatch(true),

                // [7] table schema fields less than thrift fields
                TestSpec.thrift(
                                new Event()
                                        .setUserId(0xffffL)
                                        .setTimestamp(0xffffL)
                                        .setEventType(UserEventType.BROWSE)
                                        .setDiagnostics(
                                                new Diagnostics().setIpaddress("testIPAddress")))
                        .rowType(
                                ROW(
                                        FIELD("userId", BIGINT()),
                                        FIELD("timestamp", BIGINT()),
                                        FIELD("diagnostics", ROW(FIELD("ipaddress", STRING())))))
                        .expect(Row.of(0xffffL, 0xffffL, Row.of("testIPAddress")))
                        .allMatch(false)
                        .protocol("org.apache.thrift.protocol.TCompactProtocol"),

                // [8] table schema fields more than thrift fields
                TestSpec.thrift(
                                new Event()
                                        .setUserId(0xffffL)
                                        .setTimestamp(0xffffL)
                                        .setEventType(UserEventType.BROWSE)
                                        .setDiagnostics(
                                                new Diagnostics().setIpaddress("testIPAddress")))
                        .rowType(
                                ROW(
                                        FIELD("userId", BIGINT()),
                                        FIELD("timestamp", BIGINT()),
                                        FIELD("timestamp1", BIGINT()),
                                        FIELD("diagnostics", ROW(FIELD("ipaddress", STRING())))))
                        .expect(Row.of(0xffffL, 0xffffL, null, Row.of("testIPAddress")))
                        .allMatch(false)
                        .ignoreFieldMismatch(true)
                        .protocol("org.apache.thrift.protocol.TCompactProtocol"),

                // [9] table schema fields more than thrift fields with not ignore field mismatch
                TestSpec.thrift(
                                new Event()
                                        .setUserId(0xffffL)
                                        .setTimestamp(0xffffL)
                                        .setEventType(UserEventType.BROWSE)
                                        .setDiagnostics(
                                                new Diagnostics().setIpaddress("testIPAddress")))
                        .rowType(
                                ROW(
                                        FIELD("userId", BIGINT()),
                                        FIELD("timestamp", BIGINT()),
                                        FIELD("timestamp1", BIGINT()),
                                        FIELD("diagnostics", ROW(FIELD("ipaddress", STRING())))))
                        .expect(Row.of(0xffffL, 0xffffL, null, Row.of("testIPAddress")))
                        .allMatch(false)
                        .ignoreFieldMismatch(false)
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .errorMessage("Table schema is not match the thrift class"),

                // [10] test schema field with compatible type conversion (DATE -> int)
                TestSpec.thrift(
                                new TestData()
                                        .setTestInt(
                                                (int) LocalDate.parse("1990-10-14").toEpochDay()))
                        .rowType(ROW(FIELD("testInt", DATE())))
                        .expect(Row.of(LocalDate.parse("1990-10-14")))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .allMatch(false),
                // [11] test schema field with compatible type conversion (TIME -> int)
                TestSpec.thrift(
                                new TestData()
                                        .setTestInt(
                                                (int) LocalTime.parse("12:12:43").toSecondOfDay()
                                                        * 1000))
                        .rowType(ROW(FIELD("testInt", TIME())))
                        .expect(Row.of(LocalTime.parse("12:12:43")))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .allMatch(false),

                // [12] test schema field with compatible type conversion
                // (TIMESTAMP_WITH_LOCAL_TIME_ZONE -> long)
                TestSpec.thrift(
                                new TestData()
                                        .setTestLong(
                                                Instant.parse("2021-04-18T11:56:30.123Z")
                                                        .toEpochMilli()))
                        .rowType(ROW(FIELD("testLong", TIMESTAMP_WITH_LOCAL_TIME_ZONE())))
                        .expect(Row.of(Instant.parse("2021-04-18T11:56:30.123Z")))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .allMatch(false),

                // [13] test schema field with compatible type conversion (TIMESTAMP -> long)
                TestSpec.thrift(
                                new TestData()
                                        .setTestLong(
                                                Instant.parse("2021-04-18T11:56:30.123Z")
                                                        .toEpochMilli()))
                        .rowType(ROW(FIELD("testLong", TIMESTAMP())))
                        .expect(
                                Row.of(
                                        Timestamp.valueOf("2021-04-18 11:56:30.123")
                                                .toLocalDateTime()))
                        .protocol("org.apache.thrift.protocol.TCompactProtocol")
                        .allMatch(false));
    }

    @Parameterized.Parameter public TestSpec testSpec;

    /**
     * test deserialize: 1. deserialize tBase to deserializedRowData. 2. convert deserializedRowData
     * to deserializedRow 3. check deserializedRow equals to testSpec.expect
     *
     * <p>test serialize: 1. convert expected row to originRowData. 1. serialize originRowData to
     * serializedBytes. 2. if tableSchema all match thrift class check serializedBytes equals to
     * testSpec.binary. 3. deserialize the serializedBytes to reDeserializedRowData. 4. convert
     * reDeserializedRowData to actual row. 5. check actual row equals to expected row.
     */
    @Test
    public void testSerDe() throws Exception {

        if (testSpec.errorMessage != null) {
            thrown.expect(IOException.class);
            thrown.expect(containsCause(new RuntimeException(testSpec.errorMessage)));
        }

        ThriftRowDataDeserializationSchema deserializationSchema =
                new ThriftRowDataDeserializationSchema(
                        (RowType) testSpec.rowType.getLogicalType(),
                        TypeInformation.of(RowData.class),
                        testSpec.thriftClassName,
                        testSpec.thriftProtocolName,
                        true,
                        testSpec.ignoreFieldMismatch);
        ThriftRowDataSerializationSchema serializationSchema =
                new ThriftRowDataSerializationSchema(
                        (RowType) testSpec.rowType.getLogicalType(),
                        testSpec.thriftClassName,
                        testSpec.thriftProtocolName,
                        testSpec.codeGenerator,
                        testSpec.ignoreFieldMismatch);

        deserializationSchema.open(mock(DeserializationSchema.InitializationContext.class));
        serializationSchema.open(mock(SerializationSchema.InitializationContext.class));

        DataStructureConverter<Object, Object> converter =
                DataStructureConverters.getConverter(testSpec.rowType);
        converter.open(Thread.currentThread().getContextClassLoader());

        // check deserialize
        RowData deserializedRowData = deserializationSchema.deserialize(testSpec.binary);
        Row deserializedRow = (Row) converter.toExternal(deserializedRowData);
        assertEquals(testSpec.expected, deserializedRow);

        // check serialize
        RowData originRowData = (RowData) converter.toInternal(testSpec.expected);
        byte[] serializedBytes = serializationSchema.serialize(originRowData);
        if (testSpec.allMatch) {
            assertArrayEquals(serializedBytes, testSpec.binary);
        }
        RowData reDeserializedRowData = deserializationSchema.deserialize(serializedBytes);
        Row actual = (Row) converter.toExternal(reDeserializedRowData);
        assertEquals(testSpec.expected, actual);
    }

    private static class TestSpec {
        private final TBase tBase;
        private DataType rowType;
        private Row expected;
        private byte[] binary;
        private String thriftClassName;
        private String thriftProtocolName;
        private ThriftCodeGenerator codeGenerator;
        private boolean ignoreFieldMismatch;
        private String errorMessage;
        private boolean allMatch;

        public TestSpec(TBase tBase) {
            this.tBase = tBase;
            this.thriftClassName = tBase.getClass().getName();
            this.codeGenerator = ThriftCodeGenerator.SCROOGE;
            this.allMatch = true;
            this.ignoreFieldMismatch = true;
        }

        public static TestSpec thrift(TBase tBase) {
            return new TestSpec(tBase);
        }

        TestSpec expect(Row row) throws TException {
            this.expected = row;
            generateBinary();
            return this;
        }

        TestSpec protocol(String protocol) throws TException {
            this.thriftProtocolName = protocol;
            generateBinary();
            return this;
        }

        TestSpec rowType(DataType rowType) throws TException {
            this.rowType = rowType;
            generateBinary();
            return this;
        }

        TestSpec allMatch(boolean allMatch) {
            this.allMatch = allMatch;
            return this;
        }

        TestSpec ignoreFieldMismatch(boolean ignoreFieldMismatch) {
            this.ignoreFieldMismatch = ignoreFieldMismatch;
            return this;
        }

        private void generateBinary() throws TException {
            if (this.tBase != null && this.thriftProtocolName != null && this.binary == null) {
                TSerializer tSerializer =
                        new TSerializer(ThriftUtils.getTProtocolFactory(thriftProtocolName));
                this.binary = tSerializer.serialize(tBase);
            }
        }

        TestSpec errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
    }
}
