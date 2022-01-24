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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.aws.util.AWSAsyncSinkUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link KinesisFirehoseSink} to write to Kinesis Data Firehose. More
 * details on the operation of this sink writer may be found in the doc for {@link
 * KinesisFirehoseSink}. More details on the internals of this sink writer may be found in {@link
 * AsyncSinkWriter}.
 *
 * <p>The {@link FirehoseAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class KinesisFirehoseSinkWriter<InputT> extends AsyncSinkWriter<InputT, Record> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* Name of the delivery stream in Kinesis Data Firehose */
    private final String deliveryStreamName;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous Firehose client - construction is by firehoseClientProperties */
    private final FirehoseAsyncClient client;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisFirehoseSinkWriter(
            ElementConverter<InputT, Record> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String deliveryStreamName,
            Properties firehoseClientProperties) {
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
        this.deliveryStreamName = deliveryStreamName;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = buildClient(firehoseClientProperties);
    }

    private FirehoseAsyncClient buildClient(Properties firehoseClientProperties) {
        final SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(firehoseClientProperties);

        return AWSAsyncSinkUtil.createAwsAsyncClient(
                firehoseClientProperties,
                httpClient,
                FirehoseAsyncClient.builder(),
                KinesisFirehoseConfigConstants.BASE_FIREHOSE_USER_AGENT_PREFIX_FORMAT,
                KinesisFirehoseConfigConstants.FIREHOSE_CLIENT_USER_AGENT_PREFIX);
    }

    @Override
    protected void submitRequestEntries(
            List<Record> requestEntries, Consumer<List<Record>> requestResult) {

        PutRecordBatchRequest batchRequest =
                PutRecordBatchRequest.builder()
                        .records(requestEntries)
                        .deliveryStreamName(deliveryStreamName)
                        .build();

        LOG.trace("Request to submit {} entries to KDF using KDF Sink.", requestEntries.size());

        CompletableFuture<PutRecordBatchResponse> future = client.putRecordBatch(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResult);
                    } else if (response.failedPutCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, requestResult);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(Record requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    private void handleFullyFailedRequest(
            Throwable err, List<Record> requestEntries, Consumer<List<Record>> requestResult) {
        LOG.warn(
                "KDF Sink failed to persist {} entries to KDF first request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString(),
                err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err)) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            PutRecordBatchResponse response,
            List<Record> requestEntries,
            Consumer<List<Record>> requestResult) {
        LOG.warn(
                "KDF Sink failed to persist {} entries to KDF first request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString());
        numRecordsOutErrorsCounter.inc(response.failedPutCount());

        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisFirehoseException.KinesisFirehoseFailFastException());
            return;
        }
        List<Record> failedRequestEntries = new ArrayList<>(response.failedPutCount());
        List<PutRecordBatchResponseEntry> records = response.requestResponses();

        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).errorCode() != null) {
                failedRequestEntries.add(requestEntries.get(i));
            }
        }

        requestResult.accept(failedRequestEntries);
    }

    private boolean isRetryable(Throwable err) {
        if (err instanceof CompletionException
                && err.getCause() instanceof ResourceNotFoundException) {
            getFatalExceptionCons()
                    .accept(
                            new KinesisFirehoseException(
                                    "Encountered non-recoverable exception", err));
            return false;
        }
        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisFirehoseException.KinesisFirehoseFailFastException(err));
            return false;
        }

        return true;
    }
}
