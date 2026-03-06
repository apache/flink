package com.example.flink;

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DummyAsyncSink extends AsyncSinkBase<String, String> {

    private final TokenBucketRateLimitingStrategy rateLimitingStrategy;

    public DummyAsyncSink(TokenBucketRateLimitingStrategy rateLimitingStrategy) {
        super((element, context) -> element, 10, 1, 100, 1024 * 1024, 1000, 1024);
        this.rateLimitingStrategy = rateLimitingStrategy;
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<String>> createWriter(WriterInitContext context) throws IOException {
        return new AsyncSinkWriter<String, String>(
                getElementConverter(),
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .setRateLimitingStrategy(rateLimitingStrategy)
                        .build(),
                Collections.emptyList()) {

            @Override
            protected void submitRequestEntries(List<String> requestEntries, ResultHandler<String> resultHandler) {
                // Simulate async request - complete immediately
                resultHandler.complete();
            }

            @Override
            protected long getSizeInBytes(String requestEntry) {
                return requestEntry.length();
            }
        };
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<String>> restoreWriter(WriterInitContext context, Collection<BufferedRequestState<String>> recoveredState) throws IOException {
        return createWriter(context);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<String>> getWriterStateSerializer() {
        return new AsyncSinkWriterStateSerializer<String>() {
            @Override
            protected void serializeRequestToStream(String request, DataOutputStream out) throws IOException {
                out.writeUTF(request);
            }

            @Override
            protected String deserializeRequestFromStream(long requestSize, DataInputStream in) throws IOException {
                return in.readUTF();
            }

            @Override
            public int getVersion() {
                return 1;
            }
        };
    }
}
