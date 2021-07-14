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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/** Asynchronous batch producer. */
public class DynamoDbBatchAsyncProducer implements DynamoDbProducer {

    private final DynamoDbClient client;
    private final BatchCollector batchCollector;
    private final int elementsPerBatchWrite;

    public DynamoDbBatchAsyncProducer(
            DynamoDbClient client, BatchCollector batchCollector, int elementsPerBatchWrite) {
        this.client = client;
        this.batchCollector = batchCollector;
        this.elementsPerBatchWrite = elementsPerBatchWrite;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public long getOutstandingRecordsCount() {
        return 0;
    }

    @Override
    public void flush() throws Exception {}

    @Override
    public void produce(PutItemRequest request) {}

    @Override
    public void produce(DeleteItemRequest request) {}

    @Override
    public void produce(UpdateItemRequest request) {
        throw new UnsupportedOperationException(
                "UpdateItemRequest is not supported in the batch mode. Use another type of DynamoDb producer.");
    }
}
