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

package software.amazon.flink.connectors;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AmazonKinesisDataStreamSink<InputT> extends AsyncSinkBase<InputT, PutRecordsRequestEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonKinesisDataStreamSink.class);

    private final String streamName;
    private static final KinesisAsyncClient client = KinesisAsyncClient.create();
    private final ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;

    /**
     * Basic service properties and limits. Supported requests per sec, max batch size, max items per batch, etc.
     */
    private Object serviceProperties;

    private final ElementConverter<InputT, PutRecordsRequestEntry> SIMPLE_STRING_ELEMENT_CONVERTER =
            (element, context) -> PutRecordsRequestEntry
                .builder()
                .data(SdkBytes.fromUtf8String(element.toString()))
                .partitionKey(String.valueOf(element.hashCode()))
                .build();

    @Override
    public SinkWriter<InputT, Void, Collection<PutRecordsRequestEntry>> createWriter(InitContext context, List<Collection<PutRecordsRequestEntry>> states) throws IOException {
        return new AmazonKinesisDataStreamWriter(context);
    }

    public AmazonKinesisDataStreamSink(String streamName, ElementConverter<InputT, PutRecordsRequestEntry> elementConverter, KinesisAsyncClient client) {
        this.streamName = streamName;
        this.elementConverter = elementConverter;
//        this.client = client;

        // verify that user supplied buffering strategy respects service specific limits
    }

    public AmazonKinesisDataStreamSink(String streamName) {
        this.streamName = streamName;
        this.elementConverter = SIMPLE_STRING_ELEMENT_CONVERTER;
//        this.client = KinesisAsyncClient.create();
    }

    private class AmazonKinesisDataStreamWriter extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {

        public AmazonKinesisDataStreamWriter(Sink.InitContext context) {
            super(elementConverter, context);
        }

        @Override
        protected void submitRequestEntries(List<PutRecordsRequestEntry> requestEntries, ResultFuture<PutRecordsRequestEntry> requestResult) {
            // create a batch request
            PutRecordsRequest batchRequest = PutRecordsRequest
                    .builder()
                    .records(requestEntries)
                    .streamName(streamName)
                    .build();

            LOG.info("submitRequestEntries: putRecords");

            // call api with batch request
            CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);

            // re-queue elements of failed requests
            future.whenComplete((response, err) -> {
                    if (err != null) {
                        LOG.warn("kinesis:PutRecords request failed: ", err);

                        requestResult.complete(requestEntries);

                        return;
                    }

                    if (response.failedRecordCount() > 0) {
                        LOG.info("Re-queueing {} messages", response.failedRecordCount());

                        ArrayList<PutRecordsRequestEntry> failedRequestEntries = new ArrayList<>(response.failedRecordCount());
                        List<PutRecordsResultEntry> records = response.records();

                        for (int i = 0; i < records.size(); i++) {
                            if (records.get(i).errorCode() != null) {
                                failedRequestEntries.add(requestEntries.get(i));
                            }
                        }

                        requestResult.complete(failedRequestEntries);
                    } else {
                        requestResult.complete(Collections.emptyList());
                    }
                });
        }
    }
}
