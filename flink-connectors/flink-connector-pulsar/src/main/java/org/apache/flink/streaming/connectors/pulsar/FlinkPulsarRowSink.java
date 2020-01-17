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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.pulsar.internal.DateTimeUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.META_FIELD_NAMES;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;

public class FlinkPulsarRowSink extends FlinkPulsarSinkBase<Row> {

	protected static final Logger LOG = LoggerFactory.getLogger(FlinkPulsarRowSink.class);

	protected final DataType dataType;

    private DataType valueType;

    private SerializableFunction<Row, Row> valueProjection;

    private SerializableFunction<Row, Row> metaProjection;

    private transient PulsarSerializer serializer;

    public FlinkPulsarRowSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            DataType dataType) {
        super(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                TopicKeyExtractor.DUMMY_FOR_ROW);

        this.dataType = dataType;
        createProjection();
    }

    public FlinkPulsarRowSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            DataType dataType) {
        this(adminUrl, defaultTopicName, newClientConf(serviceUrl), properties, dataType);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.serializer = new PulsarSerializer(valueType, false);
    }

    private void createProjection() {
        int[] metas = new int[3];

        FieldsDataType fdt = (FieldsDataType) dataType;
        Map<String, DataType> fdtm = fdt.getFieldDataTypes();

        List<RowType.RowField> rowFields = ((RowType) fdt.getLogicalType()).getFields();
        Map<String, Tuple2<LogicalTypeRoot, Integer>> name2Type = new HashMap<>();
        for (int i = 0; i < rowFields.size(); i++) {
            RowType.RowField rf = rowFields.get(i);
            name2Type.put(rf.getName(), new Tuple2<>(rf.getType().getTypeRoot(), i));
        }

        // topic
        if (name2Type.containsKey(TOPIC_ATTRIBUTE_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(TOPIC_ATTRIBUTE_NAME);
            if (value.f0 == LogicalTypeRoot.VARCHAR) {
                metas[0] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("attribute unsupported type %s, %s must be a string", value.f0.toString(), TOPIC_ATTRIBUTE_NAME));
            }
        } else {
            if (!forcedTopic) {
                throw new IllegalStateException(
                        String.format("topic option required when no %s attribute is present.", TOPIC_ATTRIBUTE_NAME));
            }
            metas[0] = -1;
        }

        // key
        if (name2Type.containsKey(KEY_ATTRIBUTE_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(KEY_ATTRIBUTE_NAME);
            if (value.f0 == LogicalTypeRoot.VARBINARY) {
                metas[1] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("%s attribute unsupported type %s", KEY_ATTRIBUTE_NAME, value.f0.toString()));
            }
        } else {
            metas[1] = -1;
        }

        // eventTime
        if (name2Type.containsKey(EVENT_TIME_NAME)) {
            Tuple2<LogicalTypeRoot, Integer> value = name2Type.get(EVENT_TIME_NAME);
            if (value.f0 == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                metas[2] = value.f1;
            } else {
                throw new IllegalStateException(
                        String.format("%s attribute unsupported type %s", EVENT_TIME_NAME, value.f0.toString()));
            }
        } else {
            metas[2] = -1;
        }

        List<RowType.RowField> nonInternalFields = rowFields.stream()
                .filter(f -> !META_FIELD_NAMES.contains(f.getName())).collect(Collectors.toList());

        if (nonInternalFields.size() == 1) {
            String fieldName = nonInternalFields.get(0).getName();
            valueType = fdtm.get(fieldName);
        } else {
            List<DataTypes.Field> fields = nonInternalFields.stream()
                    .map(f -> {
                        String fieldName = f.getName();
                        return DataTypes.FIELD(fieldName, fdtm.get(fieldName));
                    }).collect(Collectors.toList());
            valueType = DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
        }

        List<Integer> values = nonInternalFields.stream()
                .map(f -> name2Type.get(f.getName()).f1).collect(Collectors.toList());

        metaProjection = row -> {
            Row result = new Row(3);
            for (int i = 0; i < metas.length; i++) {
                if (metas[i] != -1) {
                    result.setField(i, row.getField(metas[i]));
                }
            }
            return result;
        };

        valueProjection = row -> {
            Row result = new Row(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.setField(i, row.getField(values.get(i)));
            }
            return result;
        };
    }

    @Override
    protected Schema<?> getPulsarSchema() {
        try {
            return SchemaUtils.sqlType2PulsarSchema(valueType);
        } catch (SchemaUtils.IncompatibleSchemaException e) {
            LOG.error(ExceptionUtils.stringifyException(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        checkErroneous();
        initializeSendCallback();

        Row metaRow = metaProjection.apply(value);
        Row valueRow = valueProjection.apply(value);
        Object v = serializer.serialize(valueRow);

        String topic;
        if (forcedTopic) {
            topic = defaultTopic;
        } else {
            topic = (String) metaRow.getField(0);
        }

        String key = (String) metaRow.getField(1);
        java.sql.Timestamp eventTime = (java.sql.Timestamp) metaRow.getField(2);

        if (topic == null) {
            if (failOnWrite) {
                throw new NullPointerException("null topic present in the data");
            }
            return;
        }

        TypedMessageBuilder builder = getProducer(topic).newMessage().value(v);

        if (key != null) {
            builder.keyBytes(key.getBytes());
        }

        if (eventTime != null) {
            long et = DateTimeUtils.fromJavaTimestamp(eventTime);
            if (et > 0) {
                builder.eventTime(et);
            }
        }

        if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }
        builder.sendAsync().whenComplete(sendCallback);
    }
}
