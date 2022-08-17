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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters.RowDataToBsonConverter;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonValue;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSink} for MongoDB. */
@Internal
public class MongoDynamicTableSink implements DynamicTableSink {

    private final MongoConnectionOptions connectionOptions;
    private final MongoWriteOptions writeOptions;
    private final DataType physicalRowDataType;
    private final SerializableFunction<RowData, BsonValue> keyExtractor;

    public MongoDynamicTableSink(
            MongoConnectionOptions connectionOptions,
            MongoWriteOptions writeOptions,
            DataType physicalRowDataType,
            SerializableFunction<RowData, BsonValue> keyExtractor) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.keyExtractor = checkNotNull(keyExtractor);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowDataToBsonConverter rowDataToBsonConverter =
                RowDataToBsonConverters.createNullableConverter(
                        physicalRowDataType.getLogicalType());

        final MongoRowDataSerializationSchema serializationSchema =
                new MongoRowDataSerializationSchema(rowDataToBsonConverter, keyExtractor);

        MongoSink<RowData> mongoSink =
                MongoSink.<RowData>builder()
                        .setUri(connectionOptions.getUri())
                        .setDatabase(connectionOptions.getDatabase())
                        .setCollection(connectionOptions.getCollection())
                        .setBulkFlushMaxActions(writeOptions.getBulkFlushMaxActions())
                        .setBulkFlushIntervalMs(writeOptions.getBulkFlushIntervalMs())
                        .setDeliveryGuarantee(writeOptions.getDeliveryGuarantee())
                        .setMaxRetryTimes(writeOptions.getMaxRetryTimes())
                        .setSerializationSchema(serializationSchema)
                        .build();

        return SinkV2Provider.of(mongoSink, writeOptions.getParallelism());
    }

    @Override
    public MongoDynamicTableSink copy() {
        return new MongoDynamicTableSink(
                connectionOptions, writeOptions, physicalRowDataType, keyExtractor);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDynamicTableSink that = (MongoDynamicTableSink) o;
        return Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(writeOptions, that.writeOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionOptions, writeOptions, physicalRowDataType);
    }
}
