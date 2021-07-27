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
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.WriteRequestFailureHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/** Asynchronous batch producer. */
public class DynamoDbBatchAsyncProducer implements DynamoDbProducer {

    private final DynamoDbClient client;
    private final BatchCollector batchCollector;
    private final WriteRequestFailureHandler failureHandler;
    private final ExecutorService taskExecutor;

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBatchAsyncProducer.class);

    private BlockingQueue<ProducerWriteRequest<DynamoDbRequest>> outgoingMessages;

    public DynamoDbBatchAsyncProducer(
            DynamoDbClient client,
            BatchCollector batchCollector,
            WriteRequestFailureHandler failureHandler,
            BlockingQueue outgoingMessages,
            ExecutorService taskExecutor) {
        this.client = client;
        this.batchCollector = batchCollector;
        this.failureHandler = failureHandler;
        this.outgoingMessages = outgoingMessages;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public long getOutstandingRecordsCount() {
        return 0;
    }

    @Override
    public void flush() throws Exception {
        batchCollector.flush();
    }

    @Override
    public void produce(PutItemRequest request) {
        writeRequest(request);
    }

    @Override
    public void produce(DeleteItemRequest request) {
        writeRequest(request);
    }

    @Override
    public void produce(UpdateItemRequest request) {
        throw new UnsupportedOperationException(
                "UpdateItemRequest is not supported in the batch mode. Use another type of DynamoDb producer.");
    }

    private void writeRequest(DynamoDbRequest request) {
        batchCollector.accumulateAndPromote(request);
    }
}
