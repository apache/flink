/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except InputT compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to InputT writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.mongodb.common.MongodbUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
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

/**
 * Sink writer created by {@link MongodbAsyncSink} to write to Mongodb collection. More details on
 * the operation of this sink writer may be found in the doc for {@link MongodbAsyncSink}. More
 * details on the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 */
@Internal
public class MongodbAsyncWriter<InputT> extends AsyncSinkWriter<InputT, Document> {
    private static final Logger LOG = LoggerFactory.getLogger(MongodbAsyncWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* Name of the mongodb database. */
    private final String databaseName;

    /* Name of the mongodb collection. */
    private final String collectionName;

    /* The sink writer metric group. */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous mongodb client. */
    private final MongoClient client;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records. */
    private final boolean failOnError;

    public MongodbAsyncWriter(
            ElementConverter<InputT, Document> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String databaseName,
            String collectionName,
            Properties mongodbClientProperties) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.failOnError = failOnError;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = buildClient(mongodbClientProperties);
    }

    private MongoClient buildClient(Properties mongodbClientProperties) {
        MongoClientSettings mongoClientSettings =
                MongodbUtil.createMongoClientSettings(mongodbClientProperties);
        return MongoClients.create(mongoClientSettings);
    }

    @Override
    protected void submitRequestEntries(
            List<Document> requestEntries, Consumer<Collection<Document>> requestResult) {
        MongoCollection<Document> collection =
                client.getDatabase(databaseName).getCollection(collectionName);

        LOG.trace("Request to submit {} entries to Mongodb Sink.", requestEntries.size());
        Mono.from(collection.insertMany(requestEntries))
                .subscribe(
                        new Consumer<InsertManyResult>() {
                            @Override
                            public void accept(InsertManyResult insertManyResult) {
                                requestResult.accept(Collections.emptyList());
                            }
                        },
                        new Consumer<Throwable>() {
                            @Override
                            public void accept(Throwable throwable) {

                                MongoBulkWriteException exception =
                                        (MongoBulkWriteException) throwable;
                                Integer insertedIndex =
                                        exception.getWriteErrors().stream()
                                                .map(x -> x.getIndex())
                                                .max((o1, o2) -> o1 - o2)
                                                .get();
                                if (failOnError) {
                                    getFatalExceptionCons()
                                            .accept(
                                                    new MongodbAsyncException
                                                            .MongodbAsyncFailFastException());
                                }
                                if (insertedIndex.equals(0)) {
                                    getFatalExceptionCons()
                                            .accept(
                                                    new MongodbAsyncException(
                                                            "Encountered non-recoverable exception",
                                                            throwable));
                                }

                                int failedRecordCount = requestEntries.size() - insertedIndex;

                                LOG.warn(
                                        "Mongodb Sink failed to persist {} entries to Mongodb",
                                        failedRecordCount);
                                numRecordsOutErrorsCounter.inc(failedRecordCount);
                                List<List<Document>> partitionLists =
                                        Lists.partition(requestEntries, insertedIndex);
                                partitionLists.stream()
                                        .skip(1)
                                        .forEach(
                                                new Consumer<List<Document>>() {
                                                    @Override
                                                    public void accept(List<Document> documents) {
                                                        requestResult.accept(documents);
                                                    }
                                                });
                            }
                        });
    }

    @Override
    protected long getSizeInBytes(Document requestEntry) {
        return requestEntry.toJson().getBytes().length;
    }
}
