package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.mongodb.internal.connection.MongoClientProvider;
import org.apache.flink.mongodb.internal.connection.MongoColloctionProviders;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Flink Sink to produce data async into a Mongodb. */
public class MongoAsyncSink<IN extends Serializable> extends AsyncSinkBase<IN, IN> {

    private final MongoClientProvider clientProvider;

    private DocumentSerializer<IN> serializer;

    public MongoAsyncSink(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS,
            String connectionString,
            String database,
            String collection,
            DocumentSerializer<IN> serializer) {
        super(
                (element, x) -> element,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS);
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(connectionString)
                        .database(database)
                        .collection(collection)
                        .build();
    }

    @Override
    public SinkWriter<IN, Void, Collection<IN>> createWriter(
            InitContext context, List<Collection<IN>> states) throws IOException {
        return new MongoWriter<IN>(context, 100, 10, 10, 10, 10, clientProvider, serializer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<IN>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
