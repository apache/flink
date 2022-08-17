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

package org.apache.flink.connector.mongodb.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;

import org.bson.BsonDocument;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link MongoSource} to make it easier for the users to construct a {@link
 * MongoSource}.
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class MongoSourceBuilder<OUT> {

    private final MongoConnectionOptions.MongoConnectionOptionsBuilder connectionOptionsBuilder;
    private final MongoReadOptions.MongoReadOptionsBuilder readOptionsBuilder;

    private List<String> projectedFields;
    private int limit = -1;
    private MongoDeserializationSchema<OUT> deserializationSchema;

    MongoSourceBuilder() {
        this.connectionOptionsBuilder = MongoConnectionOptions.builder();
        this.readOptionsBuilder = MongoReadOptions.builder();
    }

    /**
     * Sets the connection string of MongoDB.
     *
     * @param uri connection string of MongoDB
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setUri(String uri) {
        connectionOptionsBuilder.setUri(uri);
        return this;
    }

    /**
     * Sets the database to sink of MongoDB.
     *
     * @param database the database to sink of MongoDB.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setDatabase(String database) {
        connectionOptionsBuilder.setDatabase(database);
        return this;
    }

    /**
     * Sets the collection to sink of MongoDB.
     *
     * @param collection the collection to sink of MongoDB.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setCollection(String collection) {
        connectionOptionsBuilder.setCollection(collection);
        return this;
    }

    /**
     * Sets the number of documents should be fetched per round-trip when reading.
     *
     * @param fetchSize the number of documents should be fetched per round-trip when reading.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setFetchSize(int fetchSize) {
        readOptionsBuilder.setFetchSize(fetchSize);
        return this;
    }

    /**
     * Sets the batch size of MongoDB find cursor.
     *
     * @param cursorBatchSize the max batch size of find cursor.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setCursorBatchSize(int cursorBatchSize) {
        readOptionsBuilder.setCursorBatchSize(cursorBatchSize);
        return this;
    }

    /**
     * Set this option to true to prevent cursor timeout (defaults to 10 minutes).
     *
     * @param noCursorTimeout Set this option to true to prevent cursor timeout.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setNoCursorTimeout(boolean noCursorTimeout) {
        readOptionsBuilder.setNoCursorTimeout(noCursorTimeout);
        return this;
    }

    /**
     * Sets the partition strategy.
     *
     * @param partitionStrategy the strategy of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionStrategy(PartitionStrategy partitionStrategy) {
        readOptionsBuilder.setPartitionStrategy(partitionStrategy);
        return this;
    }

    /**
     * Sets the partition size of MongoDB split.
     *
     * @param partitionSize the memory size of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionSize(MemorySize partitionSize) {
        readOptionsBuilder.setPartitionSize(partitionSize);
        return this;
    }

    /**
     * Sets the samples size per partition only effective for sample partition strategy.
     *
     * @param samplesPerPartition the samples size per partition
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setSamplesPerPartition(int samplesPerPartition) {
        readOptionsBuilder.setSamplesPerPartition(samplesPerPartition);
        return this;
    }

    /**
     * Sets the limit of documents to read.
     *
     * @param limit the limit of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setLimit(int limit) {
        checkArgument(limit == -1 || limit > 0, "The limit must be larger than 0");
        this.limit = limit;
        return this;
    }

    /**
     * Sets the projection fields of documents to read.
     *
     * @param projectedFields the projection fields of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setProjectedFields(String... projectedFields) {
        checkNotNull(projectedFields, "The projected fields must be supplied");
        return setProjectedFields(Arrays.asList(projectedFields));
    }

    /**
     * Sets the projection fields of documents to read.
     *
     * @param projectedFields the projection fields of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setProjectedFields(List<String> projectedFields) {
        checkArgument(
                !isNullOrEmpty(projectedFields), "At least one projected field to be supplied");
        this.projectedFields = projectedFields;
        return this;
    }

    /**
     * Sets the deserialization schema for MongoDB {@link BsonDocument}.
     *
     * @param deserializationSchema the deserialization schema to deserialize {@link BsonDocument}.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setDeserializationSchema(
            MongoDeserializationSchema<OUT> deserializationSchema) {
        checkNotNull(deserializationSchema, "The deserialization schema must not be null");
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Build the {@link MongoSource}.
     *
     * @return a MongoSource with the settings made for this builder.
     */
    public MongoSource<OUT> build() {
        checkNotNull(deserializationSchema, "The deserialization schema must be supplied");
        return new MongoSource<>(
                connectionOptionsBuilder.build(),
                readOptionsBuilder.build(),
                projectedFields,
                limit,
                deserializationSchema);
    }
}
