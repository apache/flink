/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.config.RestartPolicy;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Asynchronous batch producer. */
public class DynamoDbBatchAsyncProducer implements DynamoDbProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBatchAsyncProducer.class);

    private final BatchCollector batchCollector;
    private BatchAsyncProcessor processor;
    private final RestartPolicy restartPolicy;
    private final BatchWriterProvider writerProvider;

    public DynamoDbBatchAsyncProducer(
            int batchSize,
            RestartPolicy restartPolicy,
            DynamoDbTablesConfig tablesConfig,
            BatchWriterProvider writerProvider) {
        this.writerProvider = writerProvider;
        this.restartPolicy = restartPolicy;
        this.processor =
                new BatchAsyncProcessor(
                        getLoopsExecutor(), writerProvider, new CompletionHandler());
        this.batchCollector = new BatchCollector(batchSize, tablesConfig, processor);
    }

    protected static ExecutorService getLoopsExecutor() {
        return Executors.newCachedThreadPool(
                (new ThreadFactoryBuilder())
                        .setDaemon(true)
                        .setNameFormat("dynamo-daemon-%04d")
                        .build());
    }

    @Override
    public void close() throws Exception {
        batchCollector.flush();
        processor.shutdown();
    }

    @Override
    public void start() {
        processor.start();
    }

    @Override
    public long getOutstandingRecordsCount() {
        return processor.getOutstandingRecordsCount();
    }

    @Override
    public void flush() throws Exception {
        batchCollector.flush();
    }

    @Override
    public void produce(PutItemRequest request) {
        batchCollector.accumulateAndPromote(request);
    }

    @Override
    public void produce(DeleteItemRequest request) {
        batchCollector.accumulateAndPromote(request);
    }

    @Override
    public void produce(UpdateItemRequest request) {
        throw new UnsupportedOperationException(
                "UpdateItemRequest is not supported in the batch mode. Use another type of DynamoDb producer.");
    }

    private class CompletionHandler implements BatchAsyncProcessor.CompletionHandler {
        @Override
        public void onCompletion(ProducerWriteResponse response) {
            LOG.debug(
                    "Finished write for batch id "
                            + response.getId()
                            + "after "
                            + response.getNumberOfAttempts()
                            + " attempts. Successful: "
                            + response.isSuccessful());
        }

        /* Called if an error occurred while scheduling or completing the write tasks.  */
        @Override
        public void onException(Throwable error) {
            LOG.error(
                    "Write failed with an unhandled exception. Restart policy set to: "
                            + restartPolicy,
                    error);

            if (restartPolicy == RestartPolicy.Shutdown) {
                LOG.info(
                        "Attempt to gracefully shutdown the processor, because 'fail on error' was set to true");
                processor.shutdown();
            } else if (restartPolicy == RestartPolicy.Restart) {
                LOG.info(
                        "Attempt to gracefully shutdown and restart the processor because of the restart policy");
                processor.shutdown();
                processor =
                        new BatchAsyncProcessor(
                                getLoopsExecutor(), writerProvider, new CompletionHandler());
                processor.start();
            }
        }
    }
}
