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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test for {@link ThriftFormatFactory}. */
public class ThriftFormatFactoryTest extends TestLogger {

    private final String thriftClassName = "org.apache.flink.formats.thrift.generated.Event";
    private final String thriftProtocolName = "org.apache.thrift.protocol.TCompactProtocol$Factory";

    @Rule public ExpectedException thrown = ExpectedException.none();

    public static List<SchemaMatchTestSpec> testData() {
        return Arrays.asList(
                // test all match type
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical("testBool", DataTypes.BOOLEAN()),
                                        Column.physical("testByte", DataTypes.TINYINT()),
                                        Column.physical("testShort", DataTypes.SMALLINT()),
                                        Column.physical("testInt", DataTypes.INT()),
                                        Column.physical("testDouble", DataTypes.DOUBLE()),
                                        Column.physical("testString", DataTypes.STRING()),
                                        Column.physical("testBinary", DataTypes.BINARY(128)),
                                        Column.physical("testLong", DataTypes.BIGINT()),
                                        Column.physical("testEnum", DataTypes.STRING()),
                                        Column.physical(
                                                "testListString",
                                                DataTypes.ARRAY(DataTypes.STRING())),
                                        Column.physical(
                                                "testSetString",
                                                DataTypes.ARRAY(DataTypes.STRING())),
                                        Column.physical(
                                                "testMapString",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.STRING())),
                                        Column.physical(
                                                "testListBinary",
                                                DataTypes.ARRAY(DataTypes.VARBINARY(1024))),
                                        Column.physical(
                                                "testMapBinary",
                                                DataTypes.MAP(
                                                        DataTypes.VARBINARY(1024),
                                                        DataTypes.VARCHAR(1024))),
                                        Column.physical(
                                                "testSetBinary",
                                                DataTypes.ARRAY(DataTypes.VARCHAR(1024)))))
                        .match(true),

                // test schema has more field than thrift
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical("testNotExistField", DataTypes.STRING())))
                        .match(false),

                // test schema field type mismatch thrift field type (list -> string)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical("testListString", DataTypes.STRING())))
                        .match(false),

                // test schema field type mismatch thrift field type (map -> array)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical(
                                                "testMapString",
                                                DataTypes.ARRAY(DataTypes.STRING()))))
                        .match(false),

                // test schema field with compatible type conversion (TIMESTAMP -> i64)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical("testLong", DataTypes.TIMESTAMP())))
                        .match(true),

                // test schema field with compatible type conversion (TIMESTAMP -> i64)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical(
                                                "testLong",
                                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())))
                        .match(true),

                // test schema field with compatible type conversion (Date -> i32)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(ResolvedSchema.of(Column.physical("testInt", DataTypes.DATE())))
                        .match(true),

                // test schema field with compatible type conversion (TIME -> i32)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(ResolvedSchema.of(Column.physical("testInt", DataTypes.TIME())))
                        .match(true),

                // test schema field with compatible type conversion (DECIMAL -> binary)
                SchemaMatchTestSpec.className("org.apache.flink.formats.thrift.generated.TestData")
                        .schema(
                                ResolvedSchema.of(
                                        Column.physical("testBinary", DataTypes.DECIMAL(10, 0))))
                        .match(true));
    }

    @Test
    public void testSchemaNotMatchThriftClass() {

        final List<SchemaMatchTestSpec> testData = testData();

        for (SchemaMatchTestSpec testSpec : testData) {
            try {
                final Map<String, String> tableOptions =
                        getModifyOptions(
                                options -> {
                                    options.put("thrift.ignore-field-mismatch", "false");
                                    options.put("thrift.class", testSpec.className);
                                });
                final DynamicTableSource actualSource =
                        createTableSource(testSpec.schema, tableOptions);
                TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                        (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE,
                        testSpec.schema.toPhysicalRowDataType());
            } catch (ValidationException e) {
                if (testSpec.isMatch) {
                    fail("failed with :" + testSpec.toString());
                }
            }
        }
    }

    @Test
    public void testSeDeSchema() {
        final Map<String, String> tableOptions = getAllOptions();

        testSchemaDeserializationSchema(tableOptions);

        testSchemaSerializationSchema(tableOptions);
    }

    @Test
    public void testInvalidThriftClass() {
        final Map<String, String> tableOptions =
                getModifyOptions(
                        options ->
                                options.put(
                                        "thrift.class",
                                        "org.apache.flink.formats.thrift.generated"));

        thrown.expect(ValidationException.class);
        thrown.expect(containsCause(new ValidationException("Could not find the needed class")));
        testSchemaSerializationSchema(tableOptions);
    }

    private static class SchemaMatchTestSpec {
        public String className;
        public ResolvedSchema schema;
        private boolean isMatch;

        private SchemaMatchTestSpec(String className) {
            this.className = className;
        }

        public static SchemaMatchTestSpec className(String className) {
            SchemaMatchTestSpec spec = new SchemaMatchTestSpec(className);
            return spec;
        }

        public SchemaMatchTestSpec schema(ResolvedSchema schema) {
            this.schema = schema;
            return this;
        }

        public SchemaMatchTestSpec match(boolean isMatch) {
            this.isMatch = isMatch;
            return this;
        }

        @Override
        public String toString() {
            return "className:"
                    + className
                    + ",schema:"
                    + schema.toString()
                    + ",isMatch:"
                    + isMatch;
        }
    }

    private void testSchemaDeserializationSchema(Map<String, String> options) {
        final ThriftRowDataDeserializationSchema expectedDeser =
                new ThriftRowDataDeserializationSchema(
                        PHYSICAL_TYPE,
                        InternalTypeInfo.of(PHYSICAL_TYPE),
                        thriftClassName,
                        thriftProtocolName,
                        false,
                        false);

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertEquals(expectedDeser, actualDeser);
    }

    private void testSchemaSerializationSchema(Map<String, String> options) {
        final ThriftRowDataSerializationSchema expectedSer =
                new ThriftRowDataSerializationSchema(
                        PHYSICAL_TYPE,
                        thriftClassName,
                        thriftProtocolName,
                        ThriftCodeGenerator.THRIFT,
                        false);

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(
                        new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE);

        assertEquals(expectedSer, actualSer);
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", ThriftFormatFactory.IDENTIFIER);
        options.put("thrift.class", thriftClassName);
        options.put("thrift.protocol", thriftProtocolName);
        return options;
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifyOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }
}
