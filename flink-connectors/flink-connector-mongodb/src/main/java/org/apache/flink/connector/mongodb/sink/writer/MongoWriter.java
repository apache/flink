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

package org.apache.flink.connector.mongodb.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContextImpl;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible to write records in a MongoDB collection.
 *
 * @param <IN> The type of the input elements.
 */
@Internal
public class MongoWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoWriter.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoWriteOptions writeOptions;
    private final MongoSerializationSchema<IN> serializationSchema;
    private final MongoSinkContext sinkContext;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final List<WriteModel<BsonDocument>> bulkRequests = new ArrayList<>();
    private final Collector<WriteModel<BsonDocument>> collector;
    private final MongoClient mongoClient;

    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0L;
    private volatile long ackTime = Long.MAX_VALUE;

    public MongoWriter(
            MongoConnectionOptions connectionOptions,
            MongoWriteOptions writeOptions,
            boolean flushOnCheckpoint,
            Sink.InitContext initContext,
            MongoSerializationSchema<IN> serializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.flushOnCheckpoint = flushOnCheckpoint;

        checkNotNull(initContext);
        this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());

        SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

        this.collector =
                new CountingCollector<>(
                        new ListCollector<>(this.bulkRequests),
                        metricGroup.getNumRecordsSendCounter());

        // Initialize the serialization schema.
        this.sinkContext = new MongoSinkContextImpl(initContext, writeOptions);
        try {
            SerializationSchema.InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            serializationSchema.open(initializationContext, sinkContext, writeOptions);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the MongoEmitter", e);
        }

        // Initialize the mongo client.
        this.mongoClient = MongoClients.create(connectionOptions.getUri());
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        collector.collect(serializationSchema.serialize(element, sinkContext));
        if (isOverMaxActionsLimit() || isOverMaxFlushIntervalLimit()) {
            doBulkWrite();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        checkpointInProgress = true;
        while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
            doBulkWrite();
        }
        checkpointInProgress = false;
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @VisibleForTesting
    void doBulkWrite() throws IOException {
        if (bulkRequests.isEmpty()) {
            // no records to write
            return;
        }

        int maxRetryTimes = writeOptions.getMaxRetryTimes();
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                lastSendTime = System.currentTimeMillis();
                mongoClient
                        .getDatabase(connectionOptions.getDatabase())
                        .getCollection(connectionOptions.getCollection(), BsonDocument.class)
                        .bulkWrite(bulkRequests);
                ackTime = System.currentTimeMillis();
                bulkRequests.clear();
                break;
            } catch (MongoException e) {
                LOG.error("Bulk Write to MongoDB failed, retry times = {}", i, e);
                if (i >= maxRetryTimes) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    private boolean isOverMaxActionsLimit() {
        int bulkActions = writeOptions.getBulkFlushMaxActions();
        return bulkActions != -1 && bulkRequests.size() >= bulkActions;
    }

    private boolean isOverMaxFlushIntervalLimit() {
        long bulkFlushInterval = writeOptions.getBulkFlushIntervalMs();
        long lastSentInterval = System.currentTimeMillis() - lastSendTime;
        return bulkFlushInterval != -1 && lastSentInterval >= bulkFlushInterval;
    }
}
