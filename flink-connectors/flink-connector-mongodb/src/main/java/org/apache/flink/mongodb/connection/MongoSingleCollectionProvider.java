package org.apache.flink.mongodb.connection;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MongoSingleCollectionProvider implements MongoClientProvider {

    /**
     * The MongoDB defaultDatabase to write to.
     */
    private final String database;

    /**
     * The defaultCollection to write to. Must be a existing defaultCollection for MongoDB 4.2 and earlier versions.
     */
    private final String defaultCollection;

    private transient MongoClient client;

    private transient MongoDatabase db;

    private transient MongoCollection<Document> collection;

    private final String servers;

    private final String username;

    private final String password;

    private final boolean retryWrites;

    private final long timeout;

    private final WriteConcern writeConcern;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSingleCollectionProvider.class);

    public MongoSingleCollectionProvider(
            String servers,
            String username,
            String password,
            WriteConcern writeConcern,
            boolean retryWrites,
            long timeout,
            String database,
            String defaultCollection) {
        this.database = database;
        this.defaultCollection = defaultCollection;
        this.servers = servers;
        this.username = username;
        this.password = password;
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
        this.timeout = timeout;
    }

    @Override
    public MongoClient getClient() {
        synchronized (this) {
            if (client == null) {
                MongoCredential credential = MongoCredential.createCredential(
                        username,
                        database,
                        password.toCharArray());
                List<ServerAddress> serverList = new ArrayList();
                String[] serverAddressArr = servers.split(",");
                for (String serverAddressStr : serverAddressArr) {
                    if (serverAddressStr.contains(":")) {
                        serverList.add(new ServerAddress(
                                serverAddressStr.split(":")[0],
                                Integer.parseInt(serverAddressStr.split(":")[1])));
                    } else {
                        serverList.add(new ServerAddress(serverAddressStr));
                    }
                }
                MongoClientSettings settings = MongoClientSettings.builder()
                        .credential(credential)
                        .applicationName("")
                        .writeConcern(writeConcern)
                        .retryWrites(retryWrites)
                        .retryReads()
                        .readPreference()
                        .compressorList()
                        .applyToClusterSettings(builder -> {
                            builder.hosts(serverList);
                            builder.serverSelectionTimeout(timeout, TimeUnit.SECONDS);
                        }).build();
                client = MongoClients.create(settings);
            }
        }
        return client;
    }

    @Override
    public MongoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (db == null) {
                db = getClient().getDatabase(database);
            }
        }
        return db;
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
