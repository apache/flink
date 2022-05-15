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

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.mongodb.util.MongoUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sink writer created by {@link MongoDbSink} to write to MongoDB. More details on the operation of
 * this sink writer may be found in the doc for {@link MongoDbSink}. More details on the internals
 * of this sink writer may be found in {@link AsyncSinkWriter}.
 */
public class MongoDbSinkWriter<InputT>
        extends AsyncSinkWriter<InputT, MongoDbWriteModel<? extends WriteModel<Document>>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbSinkWriter.class);

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsSendErrorsCounter;

    /* The reactive streams MongoDB client - construction is by mongoConfigProperties */
    private final MongoClient mongoClient;

    private final MongoCollection<Document> collection;

    /* Internally calling bulkWrite */
    private final BulkWriteOptions bulkWriteOptions;

    public MongoDbSinkWriter(
            ElementConverter<InputT, MongoDbWriteModel<? extends WriteModel<Document>>>
                    elementConverter,
            org.apache.flink.api.connector.sink2.Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            Collection<BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>>>
                    bufferedRequestStates,
            boolean failOnError,
            String databaseName,
            String collectionName,
            Properties mongoConfigProperties) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                bufferedRequestStates);
        this.failOnError = failOnError;
        this.metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();
        this.bulkWriteOptions = MongoUtils.createBulkWriteOptions(mongoConfigProperties);
        this.mongoClient = MongoUtils.createMongoClient(mongoConfigProperties);
        this.collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
    }

    @Override
    protected void submitRequestEntries(
            List<MongoDbWriteModel<? extends WriteModel<Document>>> requestEntries,
            Consumer<List<MongoDbWriteModel<? extends WriteModel<Document>>>> requestResult) {
        List<? extends WriteModel<Document>> writeModels =
                requestEntries.stream()
                        .map(MongoDbWriteModel::getWriteModel)
                        .collect(Collectors.toList());

        Mono.from(collection.bulkWrite(writeModels, bulkWriteOptions))
                .doOnNext(bulkWriteResult -> requestResult.accept(Collections.emptyList()))
                .doOnError(
                        throwable -> {
                            LOG.debug(
                                    "MongoDB Sink failed to write {} entries to MongoDB",
                                    requestEntries.size(),
                                    throwable);
                            numRecordsSendErrorsCounter.inc(requestEntries.size());
                            if (failOnError) {
                                getFatalExceptionCons()
                                        .accept(new MongoDbException.MongoDbFailFastException());
                            } else {
                                requestResult.accept(Collections.emptyList());
                            }
                        })
                .subscribe();
    }

    @Override
    protected long getSizeInBytes(MongoDbWriteModel<? extends WriteModel<Document>> requestEntry) {
        return requestEntry.getSizeInBytes();
    }
}
