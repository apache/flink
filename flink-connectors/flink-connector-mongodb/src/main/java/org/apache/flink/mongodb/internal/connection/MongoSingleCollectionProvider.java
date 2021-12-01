package org.apache.flink.mongodb.internal.connection;

import org.apache.flink.util.Preconditions;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple implementation of {@link MongoClientProvider}. */
public class MongoSingleCollectionProvider implements MongoClientProvider {

    /** Connection string to MongoDB standalone instances, replica sets or sharded clusters. */
    private final String connectionString;

    /** The MongoDB defaultDatabase to write to. */
    private final String defaultDatabase;

    /**
     * The defaultCollection to write to. Must be a existing defaultCollection for MongoDB 4.2 and
     * earlier versions.
     */
    private final String defaultCollection;

    private transient MongoClient client;

    private transient MongoDatabase database;

    private transient MongoCollection<Document> collection;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MongoSingleCollectionProvider.class);
    /** */
    public MongoSingleCollectionProvider(
            String connectionString, String defaultDatabase, String defaultCollection) {
        Preconditions.checkNotNull(connectionString);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.connectionString = connectionString;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
    }

    @Override
    public MongoClient getClient() {
        synchronized (this) {
            if (client == null) {
                client = MongoClients.create(connectionString);
            }
        }
        return client;
    }

    @Override
    public MongoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (database == null) {
                database = getClient().getDatabase(defaultDatabase);
            }
        }
        return database;
    }

    @Override
    public MongoCollection<Document> getDefaultCollection() {
        synchronized (this) {
            if (collection == null) {
                collection = getDefaultDatabase().getCollection(defaultCollection);
            }
        }
        return collection;
    }

    @Override
    public MongoClient recreateClient() {
        close();
        return getClient();
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close Mongo client", e);
        } finally {
            client = null;
        }
    }
}
