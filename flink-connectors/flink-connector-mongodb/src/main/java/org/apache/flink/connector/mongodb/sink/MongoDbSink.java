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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * A MongoDb Sink that performs async requests against a destination collection using the buffering
 * protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link com.mongodb.reactivestreams.client.MongoClient} to
 * communicate with the MongoDb endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to MongoDb
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the maximum
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to
 *       MongoDb measured in bytes
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink
 *   <li>{@code failOnError}: when an exception is encountered while persisting to Kinesis Data
 *       Streams, the job will fail immediately if failOnError is set
 * </ul>
 *
 * <p>Please see the writer implementation in {@link MongoDbSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class MongoDbSink<InputT> extends AsyncSinkBase<InputT, MongoReplaceOneModel> {

    private final boolean failOnError;
    private final String dbName;
    private final String collectionName;
    private final Properties mongoClientProperties;

    protected MongoDbSink(
            ElementConverter<InputT, MongoReplaceOneModel> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String dbName,
            String collectionName,
            Properties mongoClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.dbName =
                Preconditions.checkNotNull(
                        dbName,
                        "The database name must not be null when initializing the MongoDb Sink.");
        Preconditions.checkArgument(
                !this.dbName.isEmpty(),
                "The database name must be set when initializing the MongoDb Sink.");
        this.collectionName =
                Preconditions.checkNotNull(
                        collectionName,
                        "The collection name must not be null when initializing the MongoDb Sink.");
        Preconditions.checkArgument(
                !this.collectionName.isEmpty(),
                "The collection name must be set when initializing the MongoDb Sink.");
        this.failOnError = failOnError;
        this.mongoClientProperties = mongoClientProperties;
    }

    public static <InputT> MongoDbSinkBuilder<InputT> builder() {
        return new MongoDbSinkBuilder<>();
    }

    @Override
    public SinkWriter<InputT, Void, Collection<MongoReplaceOneModel>> createWriter(
            InitContext context, List<Collection<MongoReplaceOneModel>> states) throws IOException {
        return new MongoDbSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                dbName,
                collectionName,
                mongoClientProperties);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<MongoReplaceOneModel>>>
            getWriterStateSerializer() {
        return Optional.empty();
    }
}
