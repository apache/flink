package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.mongodb.connection.MongoClientProvider;
import org.apache.flink.mongodb.connection.MongoColloctionProviders;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.mongodb.WriteConcern.MAJORITY;

public class MongoSink<IN> implements Sink<IN, DocumentBulk, DocumentBulk, Void> {

    private DocumentSerializer<IN> serializer;
    private boolean isTransactional;
    private transient MongoClient mongoClient;
    private transient MongoDatabase db;
    private transient MongoCollection<Document> collection;
    private long maxSize;
    private long bulkFlushInterval;
    private boolean flushOnCheckpoint;
    private final MongoClientProvider clientProvider;
    private boolean model;

    public static <IN> DefaultMongoSinkBuilder<IN> BuilderClient(String username,
                                                                 String password,
                                                                 String servers,
                                                                 DocumentSerializer<IN> serializer
    ) {
        return new DefaultMongoSinkBuilder<IN>(username, password, servers, serializer);
    }

    public static final class DefaultMongoSinkBuilder<IN> {
        private final String servers;
        private final String username;
        private final String password;
        private final DocumentSerializer<IN> serializer;
        private String database;
        private String collectionName;
        private boolean isTransactional = false;
        private boolean retryWrites = true;
        private WriteConcern writeConcern = MAJORITY;
        private long timeout = 30000L;
        private long maxSize = 1024L;
        private long bulkFlushInterval = 1000L;
        private boolean flushOnCheckpoint = true;
        private boolean model = false;


        public DefaultMongoSinkBuilder(String username,
                                       String password,
                                       String servers,
                                       DocumentSerializer<IN> serializer
        ) {
            this.servers = servers;
            this.username = username;
            this.password = password;
            this.serializer = serializer;
        }

        public DefaultMongoSinkBuilder<IN> isTransactional(final Boolean isTransactional) {
            this.isTransactional = isTransactional;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> isRetryWrites(final Boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setAcknowledgmentOfWriteOperations(final WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setServerSelectionTimeout(final long timeout) {
            this.timeout = timeout;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setBulkFlushInterval(final long bulkFlushInterval) {
            this.bulkFlushInterval = bulkFlushInterval;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setMaxSize(final long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> isFlushOnCheckpoint(final Boolean flushOnCheckpoint) {
            this.flushOnCheckpoint = flushOnCheckpoint;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setDatabase(final String database) {
            this.database = database;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> setCollection(final String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DefaultMongoSinkBuilder<IN> isOpenMergeIntoModel(final boolean model) {
            this.model = model;
            return this;
        }

        public MongoSink<IN> build() {
            return new MongoSink<IN>(this.servers, this.database, this.username, this.password, this.collectionName, this.serializer
                    , this.isTransactional, this.retryWrites, this.writeConcern, this.timeout, this.maxSize, this.bulkFlushInterval,
                    this.flushOnCheckpoint,this.model);
        }
    }

    public MongoSink(String servers,
                     String database,
                     String username,
                     String password,
                     String collectionName,
                     DocumentSerializer<IN> serializer,
                     boolean isTransactional,
                     boolean retryWrites,
                     WriteConcern writeConcern,
                     long timeout,
                     long maxSize,
                     long bulkFlushInterval,
                     boolean flushOnCheckpoint,
                     boolean model

    ) {
        this.serializer = serializer;
        this.isTransactional = isTransactional;
        this.maxSize = maxSize;
        this.bulkFlushInterval = bulkFlushInterval;
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.model = model;

        this.clientProvider =
                MongoColloctionProviders
                        .getBuilder()
                        .setServers(servers)
                        .setUserName(username)
                        .setPassword(password)
                        .setdatabase(database)
                        .setWriteConcern(writeConcern)
                        .setRetryWrites(retryWrites)
                        .setTimeout(timeout)
                        .setCollection(collectionName).build();
    }


    @Override
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(InitContext initContext, List<DocumentBulk> states) {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<IN>(collection, serializer, maxSize, bulkFlushInterval, flushOnCheckpoint,model);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (isTransactional) {
            return Optional.of(new MongoCommitter(clientProvider));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        return Optional.of(new DocumentBulkSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        return Optional.of(new DocumentBulkSerializer());
    }
}
