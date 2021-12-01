package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.mongodb.internal.connection.MongoClientProvider;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class is responsible to write records in a Mongodb and to handle the different delivery.
 *
 * @param <IN> The type of the input elements.
 */
public class MongoWriter<IN extends Serializable> extends AsyncSinkWriter<IN, IN> {

    private transient MongoCollection<Document> collection;

    private final MongoClientProvider collectionProvider;

    private DocumentSerializer<IN> serializer;

    public MongoWriter(
            InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS,
            MongoClientProvider collectionProvider,
            DocumentSerializer<IN> serializer) {
        super(
                (elem, ctx) -> elem,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS);
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
    }

    @Override
    protected void submitRequestEntries(
            List<IN> requestEntries, Consumer<Collection<IN>> requestResult) {
        this.collection = collectionProvider.getDefaultCollection();
        ArrayList<Document> documents = new ArrayList<>();
        for (IN requestEntry : requestEntries) {
            Document document = serializer.serialize(requestEntry);
            documents.add(document);
        }
        Mono<InsertManyResult> result =
                Mono.from(collectionProvider.getClient().startSession())
                        .flatMap(
                                session -> {
                                    session.startTransaction();
                                    return Mono.from(collection.insertMany(session, documents))
                                            .onErrorResume(
                                                    e ->
                                                            Mono.from(session.abortTransaction())
                                                                    .then(Mono.error(e)))
                                            .flatMap(
                                                    val ->
                                                            Mono.from(session.commitTransaction())
                                                                    .then(Mono.just(val)))
                                            .doFinally(signal -> session.close());
                                });
    }

    @Override
    protected long getSizeInBytes(IN requestEntry) {
        return 0;
    }
}
