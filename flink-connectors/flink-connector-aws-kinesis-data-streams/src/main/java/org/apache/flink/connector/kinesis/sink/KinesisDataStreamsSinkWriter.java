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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.kinesis.util.AWSKinesisDataStreamsUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link KinesisDataStreamsSink} to write to Kinesis Data Streams. More
 * details on the operation of this sink writer may be found in the doc for {@link
 * KinesisDataStreamsSink}. More details on the internals of this sink writer may be found in {@link
 * AsyncSinkWriter}.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
class KinesisDataStreamsSinkWriter<InputT> extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisDataStreamsSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* Name of the stream in Kinesis Data Streams */
    private final String streamName;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous Kinesis client - construction is by kinesisClientProperties */
    private final KinesisAsyncClient client;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisDataStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            Properties kinesisClientProperties) {
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
        this.streamName = streamName;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = buildClient(kinesisClientProperties);
    }

    private KinesisAsyncClient buildClient(Properties kinesisClientProperties) {

        final SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(kinesisClientProperties);

        return AWSKinesisDataStreamsUtil.createKinesisAsyncClient(
                kinesisClientProperties, httpClient);
    }

    @Override
    protected void submitRequestEntries(
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<Collection<PutRecordsRequestEntry>> requestResult) {

        PutRecordsRequest batchRequest =
                PutRecordsRequest.builder().records(requestEntries).streamName(streamName).build();

        LOG.trace("Request to submit {} entries to KDS using KDS Sink.", requestEntries.size());

        CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResult);
                    } else if (response.failedRecordCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, requestResult);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(PutRecordsRequestEntry requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<Collection<PutRecordsRequestEntry>> requestResult) {
        LOG.warn("KDS Sink failed to persist {} entries to KDS", requestEntries.size(), err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err)) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            PutRecordsResponse response,
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<Collection<PutRecordsRequestEntry>> requestResult) {
        LOG.warn("KDS Sink failed to persist {} entries to KDS", response.failedRecordCount());
        numRecordsOutErrorsCounter.inc(response.failedRecordCount());

        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisDataStreamsException.KinesisDataStreamsFailFastException());
            return;
        }
        List<PutRecordsRequestEntry> failedRequestEntries =
                new ArrayList<>(response.failedRecordCount());
        List<PutRecordsResultEntry> records = response.records();

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
                            new KinesisDataStreamsException(
                                    "Encountered non-recoverable exception", err));
            return false;
        }
        if (failOnError) {
            getFatalExceptionCons()
                    .accept(
                            new KinesisDataStreamsException.KinesisDataStreamsFailFastException(
                                    err));
            return false;
        }

        return true;
    }
}
