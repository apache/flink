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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Abstract test base for {@link KafkaTableSourceSinkFactoryBase}. */
public abstract class KafkaTableSourceSinkFactoryTestBase extends TestLogger {

    private static final String TOPIC = "myTopic";
    private static final int PARTITION_0 = 0;
    private static final long OFFSET_0 = 100L;
    private static final int PARTITION_1 = 1;
    private static final long OFFSET_1 = 123L;
    private static final String FRUIT_NAME = "fruit-name";
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String EVENT_TIME = "event-time";
    private static final String PROC_TIME = "proc-time";
    private static final String WATERMARK_EXPRESSION = EVENT_TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);

    private static final Properties KAFKA_PROPERTIES = new Properties();

    static {
        KAFKA_PROPERTIES.setProperty("group.id", "dummy");
        KAFKA_PROPERTIES.setProperty("bootstrap.servers", "dummy");
    }

    private static final Map<Integer, Long> OFFSETS = new HashMap<>();

    static {
        OFFSETS.put(PARTITION_0, OFFSET_0);
        OFFSETS.put(PARTITION_1, OFFSET_1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSource() {
        // prepare parameters for Kafka table source
        final TableSchema schema =
                TableSchema.builder()
                        .field(FRUIT_NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(38, 18))
                        .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                        .field(PROC_TIME, DataTypes.TIMESTAMP(3))
                        .build();

        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
                Collections.singletonList(
                        new RowtimeAttributeDescriptor(
                                EVENT_TIME, new ExistingField(TIME), new AscendingTimestamps()));

        final Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put(FRUIT_NAME, NAME);
        fieldMapping.put(NAME, NAME);
        fieldMapping.put(COUNT, COUNT);
        fieldMapping.put(TIME, TIME);

        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_1), OFFSET_1);

        final DeserializationSchemaMock deserializationSchema =
                new DeserializationSchemaMock(
                        TableSchema.builder()
                                .field(NAME, DataTypes.STRING())
                                .field(COUNT, DataTypes.DECIMAL(38, 18))
                                .field(TIME, DataTypes.TIMESTAMP(3))
                                .build()
                                .toRowType());

        final KafkaTableSourceBase expected =
                getExpectedKafkaTableSource(
                        schema,
                        Optional.of(PROC_TIME),
                        rowtimeAttributeDescriptors,
                        fieldMapping,
                        TOPIC,
                        KAFKA_PROPERTIES,
                        deserializationSchema,
                        StartupMode.SPECIFIC_OFFSETS,
                        specificOffsets,
                        0L);

        TableSourceValidation.validateTableSource(expected, schema);

        // construct table source using descriptors and table source factory
        final Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.putAll(createKafkaSourceProperties());
        propertiesMap.put("schema.watermark.0.rowtime", EVENT_TIME);
        propertiesMap.put("schema.watermark.0.strategy.expr", WATERMARK_EXPRESSION);
        propertiesMap.put("schema.watermark.0.strategy.data-type", WATERMARK_DATATYPE.toString());
        propertiesMap.put("schema.4.name", COMPUTED_COLUMN_NAME);
        propertiesMap.put("schema.4.data-type", COMPUTED_COLUMN_DATATYPE.toString());
        propertiesMap.put("schema.4.expr", COMPUTED_COLUMN_EXPRESSION);

        final TableSource<?> actualSource =
                TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                        .createStreamTableSource(propertiesMap);

        assertEquals(expected, actualSource);

        // test Kafka consumer
        final KafkaTableSourceBase actualKafkaSource = (KafkaTableSourceBase) actualSource;
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        actualKafkaSource.getDataStream(mock);
        assertTrue(
                getExpectedFlinkKafkaConsumer().isAssignableFrom(mock.sourceFunction.getClass()));
        // Test commitOnCheckpoints flag should be true when set consumer group.
        assertTrue(((FlinkKafkaConsumerBase) mock.sourceFunction).getEnableCommitOnCheckpoints());
    }

    @Test
    public void testTableSourceCommitOnCheckpointsDisabled() {
        Map<String, String> propertiesMap = new HashMap<>();
        createKafkaSourceProperties()
                .forEach(
                        (k, v) -> {
                            if (!k.equals("connector.properties.group.id")) {
                                propertiesMap.put(k, v);
                            }
                        });
        final TableSource<?> tableSource =
                TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                        .createStreamTableSource(propertiesMap);
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        // Test commitOnCheckpoints flag should be false when do not set consumer group.
        ((KafkaTableSourceBase) tableSource).getDataStream(mock);
        assertTrue(mock.sourceFunction instanceof FlinkKafkaConsumerBase);
        assertFalse(((FlinkKafkaConsumerBase) mock.sourceFunction).getEnableCommitOnCheckpoints());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSourceWithLegacyProperties() {
        // prepare parameters for Kafka table source
        final TableSchema schema =
                TableSchema.builder()
                        .field(FRUIT_NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(38, 18))
                        .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                        .field(PROC_TIME, DataTypes.TIMESTAMP(3))
                        .build();

        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
                Collections.singletonList(
                        new RowtimeAttributeDescriptor(
                                EVENT_TIME, new ExistingField(TIME), new AscendingTimestamps()));

        final Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put(FRUIT_NAME, NAME);
        fieldMapping.put(NAME, NAME);
        fieldMapping.put(COUNT, COUNT);
        fieldMapping.put(TIME, TIME);

        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_1), OFFSET_1);

        final DeserializationSchemaMock deserializationSchema =
                new DeserializationSchemaMock(
                        TableSchema.builder()
                                .field(NAME, DataTypes.STRING())
                                .field(COUNT, DataTypes.DECIMAL(38, 18))
                                .field(TIME, DataTypes.TIMESTAMP(3))
                                .build()
                                .toRowType());

        final KafkaTableSourceBase expected =
                getExpectedKafkaTableSource(
                        schema,
                        Optional.of(PROC_TIME),
                        rowtimeAttributeDescriptors,
                        fieldMapping,
                        TOPIC,
                        KAFKA_PROPERTIES,
                        deserializationSchema,
                        StartupMode.SPECIFIC_OFFSETS,
                        specificOffsets,
                        0L);

        TableSourceValidation.validateTableSource(expected, schema);

        // construct table source using descriptors and table source factory
        final Map<String, String> legacyPropertiesMap = new HashMap<>();
        legacyPropertiesMap.putAll(createKafkaSourceProperties());

        // use legacy properties
        legacyPropertiesMap.remove("connector.specific-offsets");
        legacyPropertiesMap.remove("connector.properties.bootstrap.servers");
        legacyPropertiesMap.remove("connector.properties.group.id");

        // keep compatible with a specified update-mode
        legacyPropertiesMap.put("update-mode", "append");

        // legacy properties for specific-offsets and properties
        legacyPropertiesMap.put("connector.specific-offsets.0.partition", "0");
        legacyPropertiesMap.put("connector.specific-offsets.0.offset", "100");
        legacyPropertiesMap.put("connector.specific-offsets.1.partition", "1");
        legacyPropertiesMap.put("connector.specific-offsets.1.offset", "123");
        legacyPropertiesMap.put("connector.properties.0.key", "bootstrap.servers");
        legacyPropertiesMap.put("connector.properties.0.value", "dummy");
        legacyPropertiesMap.put("connector.properties.1.key", "group.id");
        legacyPropertiesMap.put("connector.properties.1.value", "dummy");

        final TableSource<?> actualSource =
                TableFactoryService.find(StreamTableSourceFactory.class, legacyPropertiesMap)
                        .createStreamTableSource(legacyPropertiesMap);

        assertEquals(expected, actualSource);

        // test Kafka consumer
        final KafkaTableSourceBase actualKafkaSource = (KafkaTableSourceBase) actualSource;
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        actualKafkaSource.getDataStream(mock);
        assertTrue(
                getExpectedFlinkKafkaConsumer().isAssignableFrom(mock.sourceFunction.getClass()));
    }

    protected Map<String, String> createKafkaSourceProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put("schema.0.data-type", "VARCHAR(2147483647)");
        map.put("schema.2.rowtime.timestamps.type", "from-field");
        map.put("connector.topic", "myTopic");
        map.put("connector.specific-offsets", "partition:0,offset:100;partition:1,offset:123");
        map.put("schema.1.name", "count");
        map.put("connector.property-version", "1");
        map.put("format.common-path", "/path/to/sth");
        map.put("schema.3.data-type", "TIMESTAMP(3)");
        map.put("schema.2.rowtime.timestamps.from", "time");
        map.put("schema.3.name", "proc-time");
        map.put("schema.0.name", "fruit-name");
        map.put("schema.2.name", "event-time");
        map.put("connector.startup-mode", "specific-offsets");
        map.put("connector.properties.group.id", "dummy");
        map.put("format.type", "test-format");
        map.put("schema.1.data-type", "DECIMAL(38, 18)");
        map.put("schema.0.from", "name");
        map.put("connector.version", getKafkaVersion());
        map.put("schema.2.rowtime.watermarks.type", "periodic-ascending");
        map.put("schema.2.data-type", "TIMESTAMP(3)");
        map.put("format.property-version", "1");
        map.put("format.derive-schema", "true");
        map.put("connector.type", "kafka");
        map.put("connector.properties.bootstrap.servers", "dummy");
        map.put("schema.3.proctime", "true");
        // test if accepted although not needed
        map.put("connector.sink-partitioner", "round-robin");
        return map;
    }

    /**
     * This test can be unified with the corresponding source test once we have fixed FLINK-9870.
     */
    @Test
    public void testTableSink() {
        // prepare parameters for Kafka table sink
        final TableSchema schema =
                TableSchema.builder()
                        .field(FRUIT_NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(10, 4))
                        .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                        .build();

        final KafkaTableSinkBase expected =
                getExpectedKafkaTableSink(
                        schema,
                        TOPIC,
                        KAFKA_PROPERTIES,
                        Optional.of(new FlinkFixedPartitioner<>()),
                        new SerializationSchemaMock(schema.toRowType()));

        // construct table sink using descriptors and table sink factory
        final Map<String, String> propertiesMap = createKafkaSinkProperties();
        final TableSink<?> actualSink =
                TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                        .createStreamTableSink(propertiesMap);

        assertEquals(expected, actualSink);

        // test Kafka producer
        final KafkaTableSinkBase actualKafkaSink = (KafkaTableSinkBase) actualSink;
        final DataStreamMock streamMock =
                new DataStreamMock(new StreamExecutionEnvironmentMock(), schema.toRowType());
        actualKafkaSink.consumeDataStream(streamMock);
        assertTrue(
                getExpectedFlinkKafkaProducer()
                        .isAssignableFrom(streamMock.sinkFunction.getClass()));
    }

    @Test
    public void testTableSinkWithLegacyProperties() {
        // prepare parameters for Kafka table sink
        final TableSchema schema =
                TableSchema.builder()
                        .field(FRUIT_NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(10, 4))
                        .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                        .build();

        final KafkaTableSinkBase expected =
                getExpectedKafkaTableSink(
                        schema,
                        TOPIC,
                        KAFKA_PROPERTIES,
                        Optional.of(new FlinkFixedPartitioner<>()),
                        new SerializationSchemaMock(schema.toRowType()));

        // construct table sink using descriptors and table sink factory
        final Map<String, String> legacyPropertiesMap = new HashMap<>();
        legacyPropertiesMap.putAll(createKafkaSinkProperties());

        // use legacy properties
        legacyPropertiesMap.remove("connector.specific-offsets");
        legacyPropertiesMap.remove("connector.properties.bootstrap.servers");
        legacyPropertiesMap.remove("connector.properties.group.id");

        // keep compatible with a specified update-mode
        legacyPropertiesMap.put("update-mode", "append");

        // legacy properties for specific-offsets and properties
        legacyPropertiesMap.put("connector.specific-offsets.0.partition", "0");
        legacyPropertiesMap.put("connector.specific-offsets.0.offset", "100");
        legacyPropertiesMap.put("connector.specific-offsets.1.partition", "1");
        legacyPropertiesMap.put("connector.specific-offsets.1.offset", "123");
        legacyPropertiesMap.put("connector.properties.0.key", "bootstrap.servers");
        legacyPropertiesMap.put("connector.properties.0.value", "dummy");
        legacyPropertiesMap.put("connector.properties.1.key", "group.id");
        legacyPropertiesMap.put("connector.properties.1.value", "dummy");

        final TableSink<?> actualSink =
                TableFactoryService.find(StreamTableSinkFactory.class, legacyPropertiesMap)
                        .createStreamTableSink(legacyPropertiesMap);

        assertEquals(expected, actualSink);

        // test Kafka producer
        final KafkaTableSinkBase actualKafkaSink = (KafkaTableSinkBase) actualSink;
        final DataStreamMock streamMock =
                new DataStreamMock(new StreamExecutionEnvironmentMock(), schema.toRowType());
        actualKafkaSink.consumeDataStream(streamMock);
        assertTrue(
                getExpectedFlinkKafkaProducer()
                        .isAssignableFrom(streamMock.sinkFunction.getClass()));
    }

    protected Map<String, String> createKafkaSinkProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put("schema.0.data-type", "VARCHAR(2147483647)");
        map.put("connector.topic", "myTopic");
        map.put("connector.specific-offsets", "partition:0,offset:100;partition:1,offset:123");
        map.put("schema.2.name", "event-time");
        // test if they accepted although not needed
        map.put("connector.startup-mode", "specific-offsets");
        map.put("update-mode", "append");
        map.put("schema.1.name", "count");
        map.put("connector.properties.group.id", "dummy");
        map.put("connector.property-version", "1");
        map.put("format.type", "test-format");
        map.put("schema.1.data-type", "DECIMAL(10, 4)");
        map.put("format.common-path", "/path/to/sth");
        map.put("connector.version", getKafkaVersion());
        map.put("schema.2.data-type", "TIMESTAMP(3)");
        map.put("format.property-version", "1");
        map.put("format.derive-schema", "true");
        map.put("connector.type", "kafka");
        map.put("connector.properties.bootstrap.servers", "dummy");
        map.put("connector.sink-partitioner", "fixed");
        map.put("schema.0.name", "fruit-name");
        return map;
    }

    // --------------------------------------------------------------------------------------------
    // For version-specific tests
    // --------------------------------------------------------------------------------------------

    protected abstract String getKafkaVersion();

    protected abstract Class<FlinkKafkaConsumerBase<Row>> getExpectedFlinkKafkaConsumer();

    protected abstract Class<?> getExpectedFlinkKafkaProducer();

    protected abstract KafkaTableSourceBase getExpectedKafkaTableSource(
            TableSchema schema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Map<String, String> fieldMapping,
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis);

    protected abstract KafkaTableSinkBase getExpectedKafkaTableSink(
            TableSchema schema,
            String topic,
            Properties properties,
            Optional<FlinkKafkaPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema);

    // --------------------------------------------------------------------------------------------
    // Mocks
    // --------------------------------------------------------------------------------------------

    private static class StreamExecutionEnvironmentMock extends StreamExecutionEnvironment {

        public SourceFunction<?> sourceFunction;

        @Override
        public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> sourceFunction) {
            this.sourceFunction = sourceFunction;
            return super.addSource(sourceFunction);
        }

        @Override
        public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static class DataStreamMock extends DataStream<Row> {

        public SinkFunction<?> sinkFunction;

        public DataStreamMock(
                StreamExecutionEnvironment environment, TypeInformation<Row> outType) {
            super(environment, new TransformationMock("name", outType, 1));
        }

        @Override
        public DataStreamSink<Row> addSink(SinkFunction<Row> sinkFunction) {
            this.sinkFunction = sinkFunction;
            return super.addSink(sinkFunction);
        }
    }

    private static class TransformationMock extends Transformation<Row> {

        public TransformationMock(String name, TypeInformation<Row> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return null;
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }

    /** Legacy format serialization schema for testing. */
    private static class SerializationSchemaMock implements SerializationSchema<Row> {

        private final TypeInformation<Row> rowTypeInfo;

        public SerializationSchemaMock(TypeInformation<Row> rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
        }

        @Override
        public byte[] serialize(Row element) {
            return new byte[0];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SerializationSchemaMock that = (SerializationSchemaMock) o;
            return rowTypeInfo.equals(that.rowTypeInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowTypeInfo);
        }
    }

    /** Legacy format deserialization schema for testing. */
    private static class DeserializationSchemaMock implements DeserializationSchema<Row> {

        private final TypeInformation<Row> rowTypeInfo;

        public DeserializationSchemaMock(TypeInformation<Row> rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
        }

        @Override
        public Row deserialize(byte[] message) throws IOException {
            return null;
        }

        @Override
        public boolean isEndOfStream(Row nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return rowTypeInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeserializationSchemaMock that = (DeserializationSchemaMock) o;
            return rowTypeInfo.equals(that.rowTypeInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowTypeInfo);
        }
    }

    /** Legacy format factory for testing. */
    public static class TestFormatFactory extends TableFormatFactoryBase<Row>
            implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

        public TestFormatFactory() {
            super("test-format", 1, true);
        }

        @Override
        protected List<String> supportedFormatProperties() {
            return Arrays.asList("format.unique-property", "format.common-path");
        }

        @Override
        public DeserializationSchema<Row> createDeserializationSchema(
                Map<String, String> properties) {
            return new DeserializationSchemaMock(deriveSchema(properties).toRowType());
        }

        @Override
        public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
            return new SerializationSchemaMock(deriveSchema(properties).toRowType());
        }
    }
}
