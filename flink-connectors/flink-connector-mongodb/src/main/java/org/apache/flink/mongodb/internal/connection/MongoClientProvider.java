package org.apache.flink.mongodb.internal.connection;

import org.apache.flink.annotation.Internal;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;

import java.io.Serializable;

/** Provided for initiate and recreate {@link MongoClient}. */
@Internal
public interface MongoClientProvider extends Serializable {

    /**
     * Create one or get the current {@link MongoClient}.
     *
     * @return Current {@link MongoClient}.
     */
    MongoClient getClient();

    /**
     * Get the default database.
     *
     * @return Current {@link MongoDatabase}.
     */
    MongoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     *
     * @return Current {@link MongoCollection}.
     */
    MongoCollection<Document> getDefaultCollection();

    /**
     * Recreate a client. Used typically when a connection is timed out or lost.
     *
     * @return A new {@link MongoClient}.
     */
    MongoClient recreateClient();

    /** Close the underlying MongoDB connection. */
    void close();
}
