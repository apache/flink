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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.dynamodb.config.ProducerType;
import org.apache.flink.streaming.connectors.dynamodb.config.RestartPolicy;
import org.apache.flink.streaming.connectors.dynamodb.util.AwsV2Util;
import org.apache.flink.streaming.connectors.dynamodb.util.TimeoutLatch;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * DynamoDB sink that writes multiple {@link WriteRequest WriteRequests} to DynamoDB for each
 * incoming element.
 *
 * <p>You also have to provide a {@link DynamoDbSinkFunction}. This is used to create a {@link
 * WriteRequest WriteRequest} for each incoming element. See the class level documentation of {@link
 * DynamoDbSinkFunction} for an example.
 *
 * @param <IN> Type of the elements handled by this sink
 */
@PublicEvolving
public class DynamoDbSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    public static final String DYNAMO_DB_PRODUCER_METRIC_GROUP = "dynamoDBProducer";

    public static final String METRIC_BACKPRESSURE_CYCLES = "backpressureCycles";

    public static final String METRIC_OUTSTANDING_RECORDS_COUNT = "outstandingRecordsCount";

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSink.class);

    /**
     * Properties to parametrize settings such as AWS service region, access key etc. These
     * properties would also be forwarded when creating the DynamoDB client
     */
    private final Properties configProps;

    /**
     * The function that is used to construct a {@link WriteRequest WriteRequest} from each incoming
     * element.
     */
    private final DynamoDbSinkFunction<IN> dynamoDBSinkFunction;

    /** A flag controlling the error behavior of the sink. */
    private boolean failOnError = true;

    /** Maximum length of the internal record queue before backpressuring. */
    private int queueLimit = Integer.MAX_VALUE;

    private int batchSize = 25;

    /**
     * Restart policy modifies behaviour of the producer if it has failed while scheduling or
     * completing tasks.
     */
    private RestartPolicy restartPolicy = RestartPolicy.Shutdown;

    /** User defined action on how to handle failed dynamodb write requests. */
    private final WriteRequestFailureHandler failureHandler;

    /** Counts how often we have to wait for KPL because we are above the queue limit. */
    private transient Counter backpressureCycles;

    /** Backpressuring waits for this latch, triggered by record callback. */
    private transient volatile TimeoutLatch backpressureLatch;

    /** Field for async exception. */
    private transient volatile Throwable thrownException;

    /** DynamoDb Client created by AWS2Util. */
    private transient DynamoDbClient client;

    private transient DynamoDbProducer producer;

    public DynamoDbSink(
            DynamoDbSinkFunction<IN> dynamoDBSinkFunction,
            Properties configProps,
            WriteRequestFailureHandler failureHandler) {
        checkNotNull(configProps, "configProps can not be null");
        this.configProps = configProps;
        checkNotNull(dynamoDBSinkFunction, "DynamoDB sink function cannot be null");
        this.dynamoDBSinkFunction = dynamoDBSinkFunction;

        checkNotNull(failureHandler, "WriteRequestFailureHandler must be set");
        this.failureHandler = failureHandler;

        // we eagerly check if the user-provided sink function is serializable;
        // otherwise, if it isn't serializable, users will merely get a non-informative error
        // message
        // "DynamoDBSink is not serializable"
        checkArgument(
                InstantiationUtil.isSerializable(dynamoDBSinkFunction),
                "The implementation of the provided DynamoDBSinkFunction is not serializable. "
                        + "The object probably contains or references non-serializable fields.");
    }

    /**
     * Constructs DynamoDB Sink with the default WriteRequestFailure header which rethrows errors
     * that occurs during the write request processing.
     */
    public DynamoDbSink(DynamoDbSinkFunction<IN> dynamoDBSinkFunction, Properties configProps) {
        this(dynamoDBSinkFunction, configProps, new DefaultFailureHandler());
    }

    /**
     * If set to true, the producer will immediately fail with an exception on any error. Otherwise,
     * the failed request is handled by the user provided WriteRequestFailureHandler.
     *
     * @param failOnError Error behavior flag
     */
    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    /**
     * Restart policy modifies behaviour of the producer if it has failed while scheduling or
     * completing tasks.
     */
    public void setRestartPolicy(RestartPolicy policy) {
        this.restartPolicy = policy;
    }

    /**
     * The {@link DynamoDbProducer} holds an unbounded queue internally. To avoid memory problems
     * under high loads, a limit can be employed above which the internal queue will be flushed,
     * thereby applying backpressure.
     *
     * @param queueLimit The maximum length of the internal queue before backpressuring
     */
    public void setQueueLimit(int queueLimit) {
        checkArgument(queueLimit > 0, "queueLimit must be a positive number");
        this.queueLimit = queueLimit;
    }

    /** @param batchSize Batch size of batch request sent to dynamodb, max is 25 */
    public void setBatchSize(int batchSize) {
        checkArgument(batchSize > 0 && batchSize <= 25, "batchSize must be between 1 and 25");
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) {
        backpressureLatch = new TimeoutLatch();
        client = AwsV2Util.createDynamoDbClient(configProps);
        this.producer = buildDynamoDBProducer(new DynamoDbProducerListener());
        try {
            this.producer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        final MetricGroup dynamoDBSinkMetricGroup =
                getRuntimeContext().getMetricGroup().addGroup(DYNAMO_DB_PRODUCER_METRIC_GROUP);
        this.backpressureCycles = dynamoDBSinkMetricGroup.counter(METRIC_BACKPRESSURE_CYCLES);
        dynamoDBSinkMetricGroup.gauge(
                METRIC_OUTSTANDING_RECORDS_COUNT, producer::getOutstandingRecordsCount);
        LOG.info("Started DynamoDB sink");
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        if (producer == null) {
            throw new NullPointerException("DynamoDB producer has been closed");
        }
        checkAndPropagateAsyncError();
        boolean didWaitForFlush = enforceQueueLimit();
        if (didWaitForFlush) {
            checkAndPropagateAsyncError();
        }
        dynamoDBSinkFunction.process(value, getRuntimeContext(), producer);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing sink");
        super.close();
        if (producer != null) {
            LOG.info("Flushing outstanding {} records", producer.getOutstandingRecordsCount());
            // try to flush all outstanding records
            flushSync();

            LOG.info("Flushing done. Destroying producer instance.");
            producer.close();
            producer = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
        checkAndPropagateAsyncError();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        // check for asynchronous errors and fail the checkpoint if necessary
        checkAndPropagateAsyncError();
        flushSync();
        if (producer.getOutstandingRecordsCount() > 0) {
            throw new IllegalStateException(
                    "Number of outstanding records must be zero at this point: "
                            + producer.getOutstandingRecordsCount());
        }
        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkAndPropagateAsyncError();
    }

    /**
     * Creates a {@link DynamoDbProducer}. Exposed so that tests can inject mock producers easily.
     */
    @VisibleForTesting
    protected DynamoDbProducer buildDynamoDBProducer(DynamoDbProducer.Listener listener) {
        return new DynamoDbProducerBuilder(client, ProducerType.BatchAsync)
                .setQueueLimit(queueLimit)
                .setListener(listener)
                .setBatchSize(batchSize)
                .setRestartPolicy(restartPolicy)
                .build();
    }

    /** Check if there are any asynchronous exceptions. If so, rethrow the exception. */
    private void checkAndPropagateAsyncError() throws Exception {
        if (thrownException != null) {
            if (failOnError) {
                throw new RuntimeException(
                        "An exception was thrown while processing a record.", thrownException);
            } else {
                LOG.warn(
                        "An exception was thrown while processing a record. Producer won't stop writing, as FailOnError was false",
                        thrownException);
                // reset, prevent double throwing
                thrownException = null;
            }
        }
    }

    /**
     * If the internal queue of the {@link DynamoDbProducer} gets too long, flush some of the
     * records until we are below the limit again. We don't want to flush _all_ records at this
     * point since that would break record aggregation.
     *
     * @return boolean whether flushing occurred or not
     */
    private boolean enforceQueueLimit() {
        int attempt = 0;
        while (producer.getOutstandingRecordsCount() >= queueLimit) {
            backpressureCycles.inc();
            if (attempt >= 10) {
                LOG.warn(
                        "Waiting for the queue length to drop below the limit takes unusually long, still not done after {} attempts.",
                        attempt);
            }
            attempt++;
            try {
                backpressureLatch.await(100);
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
        return attempt > 0;
    }

    /** Releases the block on flushing if an interruption occurred. */
    private void flushSync() throws Exception {
        while (producer.getOutstandingRecordsCount() > 0) {
            producer.flush();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
    }

    private class DynamoDbProducerListener implements DynamoDbProducer.Listener {

        @Override
        public void beforeWrite(String executionId, ProducerWriteRequest request) {
            LOG.debug("Start writing request: {}", request);
        }

        @Override
        public void afterWrite(
                String executionId, ProducerWriteRequest request, ProducerWriteResponse response) {
            backpressureLatch.trigger();
            if (!response.isSuccessful()) {
                try {
                    failureHandler.onFailure(request, response);
                } catch (Throwable throwable) {
                    LOG.debug("Write request filed for execution id {}", executionId);
                    thrownException = throwable;
                }
            }
        }

        @Override
        public void afterWrite(
                String executionId, ProducerWriteRequest request, Throwable failure) {
            backpressureLatch.trigger();
            try {
                failureHandler.onFailure(request, failure);
            } catch (Throwable throwable) {
                LOG.debug("Write request filed for execution id {}", executionId);
                thrownException = throwable;
            }
        }
    }

    /**
     * This implementation of failure handler is used if user didn't provide own implementation to
     * process failed write requests.
     */
    private static class DefaultFailureHandler implements WriteRequestFailureHandler {

        @Override
        public void onFailure(ProducerWriteRequest request, Throwable failure) throws Throwable {
            LOG.error("Write request failed", failure);
            throw new Exception(failure);
        }

        @Override
        public void onFailure(ProducerWriteRequest request, ProducerWriteResponse response)
                throws Throwable {
            LOG.error("Write request failed", response.getException());
            throw new Exception(response.getException());
        }
    }
}
