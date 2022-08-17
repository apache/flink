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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.util.InstantiationUtil;

import com.mongodb.client.model.WriteModel;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base builder to construct a {@link MongoSink}.
 *
 * @param <IN> type of the records converted to MongoDB bulk request
 */
@PublicEvolving
public class MongoSinkBuilder<IN> {

    private final MongoConnectionOptions.MongoConnectionOptionsBuilder connectionOptionsBuilder;
    private final MongoWriteOptions.MongoWriteOptionsBuilder writeOptionsBuilder;

    private MongoSerializationSchema<IN> serializationSchema;

    MongoSinkBuilder() {
        this.connectionOptionsBuilder = MongoConnectionOptions.builder();
        this.writeOptionsBuilder = MongoWriteOptions.builder();
    }

    /**
     * Sets the connection string of MongoDB.
     *
     * @param uri connection string of MongoDB
     * @return this builder
     */
    public MongoSinkBuilder<IN> setUri(String uri) {
        connectionOptionsBuilder.setUri(uri);
        return this;
    }

    /**
     * Sets the database to sink of MongoDB.
     *
     * @param database the database to sink of MongoDB.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setDatabase(String database) {
        connectionOptionsBuilder.setDatabase(database);
        return this;
    }

    /**
     * Sets the collection to sink of MongoDB.
     *
     * @param collection the collection to sink of MongoDB.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setCollection(String collection) {
        connectionOptionsBuilder.setCollection(collection);
        return this;
    }

    /**
     * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
     * disable it. The default flush size 1000.
     *
     * @param numMaxActions the maximum number of actions to buffer per bulk request.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setBulkFlushMaxActions(int numMaxActions) {
        writeOptionsBuilder.setBulkFlushMaxActions(numMaxActions);
        return this;
    }

    /**
     * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setBulkFlushIntervalMs(long intervalMillis) {
        writeOptionsBuilder.setBulkFlushIntervalMs(intervalMillis);
        return this;
    }

    /**
     * Sets the max retry times if writing records failed.
     *
     * @param maxRetryTimes the max retry times.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setMaxRetryTimes(int maxRetryTimes) {
        writeOptionsBuilder.setMaxRetryTimes(maxRetryTimes);
        return this;
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#NONE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public MongoSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        writeOptionsBuilder.setDeliveryGuarantee(deliveryGuarantee);
        return this;
    }

    /**
     * Sets the serialization schema which is invoked on every record to convert it to MongoDB bulk
     * request.
     *
     * @param serializationSchema to process records into MongoDB bulk {@link WriteModel}.
     * @return this builder
     */
    public MongoSinkBuilder<IN> setSerializationSchema(
            MongoSerializationSchema<IN> serializationSchema) {
        checkNotNull(serializationSchema);
        checkState(
                InstantiationUtil.isSerializable(serializationSchema),
                "The mongo serialization schema must be serializable.");
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * Constructs the {@link MongoSink} with the properties configured this builder.
     *
     * @return {@link MongoSink}
     */
    public MongoSink<IN> build() {
        checkNotNull(serializationSchema, "The serialization schema must be supplied");
        return new MongoSink<>(
                connectionOptionsBuilder.build(), writeOptionsBuilder.build(), serializationSchema);
    }
}
