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
