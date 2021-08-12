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

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.streaming.connectors.dynamodb.batch.BatchWriterProvider;
import org.apache.flink.streaming.connectors.dynamodb.batch.DynamoDbBatchAsyncProducer;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.config.ProducerType;
import org.apache.flink.streaming.connectors.dynamodb.retry.BatchWriterRetryPolicy;
import org.apache.flink.streaming.connectors.dynamodb.retry.DefaultBatchWriterRetryPolicy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Constructs dynamodb producer. */
public class DynamoDbProducerBuilder {

    public static final int DEFAULT_BATCH_SIZE = 25;
    public static final int DEFAULT_MAX_NUMBER_OF_RETRY_ATTEMPTS = 50;
    public static final int DEFAULT_INTERNAL_QUEUE_LIMIT = 1000;

    private final DynamoDbClient client;
    private ProducerType type;
    private DynamoDbTablesConfig tablesConfig;
    private int batchSize;
    private int queueLimit;
    private BatchWriterRetryPolicy retryPolicy;
    private DynamoDbProducer.Listener listener;
    private boolean failOnError;

    public DynamoDbProducerBuilder(DynamoDbClient client, ProducerType type, boolean failOnError) {
        this.client = client;
        this.type = type;
        this.failOnError = failOnError;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.queueLimit = DEFAULT_INTERNAL_QUEUE_LIMIT;
        this.retryPolicy = new DefaultBatchWriterRetryPolicy(DEFAULT_MAX_NUMBER_OF_RETRY_ATTEMPTS);
    }

    public DynamoDbProducerBuilder setTablesConfig(DynamoDbTablesConfig config) {
        this.tablesConfig = config;
        return this;
    }

    public DynamoDbProducerBuilder setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public DynamoDbProducerBuilder setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
        return this;
    }

    // TODO define retry policy in a better way
    public DynamoDbProducerBuilder setRetryPolicy(BatchWriterRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public DynamoDbProducerBuilder setListener(DynamoDbProducer.Listener listener) {
        this.listener = listener;
        return this;
    }

    public DynamoDbProducerBuilder setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public DynamoDbProducer build() {
        if (type == ProducerType.BATCH_ASYNC) {
            BatchWriterProvider writerProvider =
                    new BatchWriterProvider(client, retryPolicy, listener);
            return new DynamoDbBatchAsyncProducer(
                    batchSize, queueLimit, failOnError, tablesConfig, writerProvider);
        } else {
            throw new RuntimeException("Producer type " + type + "is not supported");
        }
    }
}
