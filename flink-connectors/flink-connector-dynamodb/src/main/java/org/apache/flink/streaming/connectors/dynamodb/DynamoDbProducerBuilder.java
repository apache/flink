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
import org.apache.flink.streaming.connectors.dynamodb.batch.retry.DefaultBatchWriterRetryPolicy;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.config.ProducerType;
import org.apache.flink.streaming.connectors.dynamodb.config.RestartPolicy;
import org.apache.flink.streaming.connectors.dynamodb.retry.WriterRetryPolicy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Constructs dynamodb producer. */
public class DynamoDbProducerBuilder {

    public static final int DEFAULT_BATCH_SIZE = 25;
    public static final int DEFAULT_INTERNAL_QUEUE_LIMIT = 1000;

    private final DynamoDbClient client;
    private final ProducerType type;
    private DynamoDbTablesConfig tablesConfig;
    private int batchSize;
    private int queueLimit;
    private WriterRetryPolicy retryPolicy;
    private DynamoDbProducer.Listener listener;
    private RestartPolicy restartPolicy;

    public DynamoDbProducerBuilder(DynamoDbClient client, ProducerType type) {
        this.client = client;
        this.type = type;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.queueLimit = DEFAULT_INTERNAL_QUEUE_LIMIT;
        this.retryPolicy = new DefaultBatchWriterRetryPolicy();
    }

    /**
     * DynamoDb tables configuration. The configuration can be used by batching producers to
     * deduplicated messages in the batch based on the value of the table keys (Primary or Composite
     * keys). If configuration is not provided deduplication will not happen and the batch write may
     * be rejected by DynamoDB.
     */
    public DynamoDbProducerBuilder setTablesConfig(DynamoDbTablesConfig config) {
        this.tablesConfig = config;
        return this;
    }

    /**
     * The batch size for the batching DynamoDB producer. The parameter only considered for the
     * batch producers, otherwise ignored. Maximum value 25. Default value 25.
     */
    public DynamoDbProducerBuilder setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /** Limit for the internal queue of the write tasks. */
    public DynamoDbProducerBuilder setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
        return this;
    }

    /**
     * Retry and backoff policy for the writer. If the policy is not set, default policy is used
     * that will retry forever until the write is successful (unless a non-retryable error).
     */
    public DynamoDbProducerBuilder setRetryPolicy(WriterRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    /**
     * Allows to set custom listener implementation. Listener is called before and after the write.
     */
    public DynamoDbProducerBuilder setListener(DynamoDbProducer.Listener listener) {
        this.listener = listener;
        return this;
    }

    /**
     * If restart policy is FailOnError, producer will attempt to do a clean shutdown (processing
     * remaining items in the queue) in case of the write error. If restart policy is Restart, the
     * producer will additionally attempt to restart the process.
     */
    public DynamoDbProducerBuilder setRestartPolicy(RestartPolicy policy) {
        this.restartPolicy = policy;
        return this;
    }

    public DynamoDbProducer build() {
        if (type == ProducerType.BatchAsync) {
            BatchWriterProvider writerProvider =
                    new BatchWriterProvider(client, retryPolicy, listener);
            return new DynamoDbBatchAsyncProducer(
                    batchSize, queueLimit, restartPolicy, tablesConfig, writerProvider);
        } else {
            throw new RuntimeException("Producer type " + type + "is not supported");
        }
    }
}
