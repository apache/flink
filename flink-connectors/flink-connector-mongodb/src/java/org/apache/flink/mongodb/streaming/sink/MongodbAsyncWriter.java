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

package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.mongodb.internal.connection.MongoClientProvider;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link MongodbAsyncSink} to write to Mongodb collection. More details on
 * the operation of this sink writer may be found in the doc for {@link MongodbAsyncSink}. More
 * details on the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link MongoClientProvider} used here may be configured in the standard way for the
 * MongoDriver 4.2.2
 */
@Internal
public class MongodbAsyncWriter<InputT> extends AsyncSinkWriter<InputT, Document> {
    private static final Logger LOG = LoggerFactory.getLogger(MongodbAsyncWriter.class);

    private final MongoClientProvider collectionProvider;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    public MongodbAsyncWriter(
            ElementConverter<InputT, Document> elementConverter,
            InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            MongoClientProvider collectionProvider,
            boolean failOnError) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.collectionProvider = collectionProvider;
        this.failOnError = failOnError;
    }

    @Override
    protected void submitRequestEntries(
            List<Document> requestEntries, Consumer<Collection<Document>> requestResult) {
        MongoCollection<Document> collection = collectionProvider.getDefaultCollection();
        Mono.from(collection.insertMany(requestEntries))
                .subscribe(
                        new Consumer<InsertManyResult>() {
                            @Override
                            public void accept(InsertManyResult insertManyResult) {
                                LOG.info(
                                        "Whether the data insertion is successful:{}",
                                        insertManyResult.wasAcknowledged());
                            }
                        },
                        new Consumer<Throwable>() {
                            @Override
                            public void accept(Throwable throwable) {
                                requestResult.accept(Collections.emptyList());
                            }
                        });
    }

    @Override
    protected long getSizeInBytes(Document requestEntry) {
        return requestEntry.toJson().getBytes().length;
    }
}
