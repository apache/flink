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
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.mongodb.util.MongoGeneralUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sink writer created by {@link MongoDbSink} to write to MongoDb Collection. More details on the
 * operation of this sink writer may be found in the doc for {@link MongoDbSink}. More details on
 * the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 */
public class MongoDbSinkWriter<InputT> extends AsyncSinkWriter<InputT, MongoReplaceOneModel> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous Mongo client - construction is by mongoClientProperties */
    private final MongoClient client;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    /* The Mongo collection to write to */
    private final MongoCollection<Document> collection;

    public MongoDbSinkWriter(
            ElementConverter<InputT, MongoReplaceOneModel> elementConverter,
            org.apache.flink.api.connector.sink.Sink.InitContext context,
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
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.failOnError = failOnError;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = buildClient(mongoClientProperties);
        this.collection = client.getDatabase(dbName).getCollection(collectionName);
    }

    @Override
    protected void submitRequestEntries(
            List<MongoReplaceOneModel> requestEntries,
            Consumer<List<MongoReplaceOneModel>> requestResult) {
        LOG.trace(
                "Request to submit {} entries to MongoDb using MongoDb Sink.",
                requestEntries.size());
        final List<WriteModel<Document>> writeModels =
                requestEntries.stream()
                        .map(MongoWriteModel::getWriteModel)
                        .collect(Collectors.toList());
        collection
                .bulkWrite(writeModels)
                .subscribe(new BulkWriteSubscriber(requestEntries, requestResult));
    }

    @Override
    protected long getSizeInBytes(MongoReplaceOneModel requestEntry) {
        final RawBsonDocument rawBsonDocument =
                RawBsonDocument.parse(requestEntry.getWriteModel().getReplacement().toJson());
        final RawBsonDocument filterDocument =
                RawBsonDocument.parse(
                        requestEntry.getWriteModel().getFilter().toBsonDocument().toJson());
        return rawBsonDocument.getByteBuffer().remaining()
                + filterDocument.getByteBuffer().remaining();
    }

    private MongoClient buildClient(Properties mongoClientProperties) {
        return MongoGeneralUtil.createMongoClient(mongoClientProperties);
    }

    private class BulkWriteSubscriber implements Subscriber<BulkWriteResult> {

        private final List<MongoReplaceOneModel> requestEntries;
        private final Consumer<List<MongoReplaceOneModel>> requestResult;

        public BulkWriteSubscriber(
                final List<MongoReplaceOneModel> requestEntries,
                final Consumer<List<MongoReplaceOneModel>> requestResult) {
            this.requestEntries = requestEntries;
            this.requestResult = requestResult;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(1);
        }

        @Override
        public void onNext(BulkWriteResult bulkWriteResult) {
            requestResult.accept(Collections.emptyList());
        }

        @Override
        public void onError(Throwable t) {
            numRecordsOutErrorsCounter.inc(requestEntries.size());
            LOG.warn(
                    "MongoDb Sink failed to persist {} entries to MongoDb",
                    requestEntries.size(),
                    t);

            if (failOnError) {
                getFatalExceptionCons()
                        .accept(new MongoDbException.MongoDbExceptionFailFastException(t));
            } else {
                requestResult.accept(requestEntries);
            }
        }

        @Override
        public void onComplete() {}
    }
}
