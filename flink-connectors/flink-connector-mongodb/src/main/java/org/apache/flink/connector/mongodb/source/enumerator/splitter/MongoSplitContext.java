/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.AVG_OBJ_SIZE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.COUNT_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SHARDED_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SIZE_FIELD;

/**
 * The split context used by {@link MongoSplitters.MongoSplitter} to split collection into a set of
 * chunks for MongoDB data source.
 */
@Internal
public class MongoSplitContext {

    /** Read options of MongoDB. */
    private final MongoReadOptions readOptions;

    /** Client of MongoDB. */
    private final MongoClient mongoClient;

    /** Namespace of MongoDB, eg. db.coll. */
    private final MongoNamespace namespace;

    /** Is a sharded collection. */
    private final boolean sharded;

    /** The number of objects or documents in this collection. */
    private final long count;

    /** The total uncompressed size(bytes) in memory of all records in a collection. */
    private final long size;

    /** The average size(bytes) of an object in the collection. */
    private final long avgObjSize;

    public MongoSplitContext(
            MongoReadOptions readOptions,
            MongoClient mongoClient,
            MongoNamespace namespace,
            boolean sharded,
            long count,
            long size,
            long avgObjSize) {
        this.readOptions = readOptions;
        this.mongoClient = mongoClient;
        this.namespace = namespace;
        this.sharded = sharded;
        this.count = count;
        this.size = size;
        this.avgObjSize = avgObjSize;
    }

    public static MongoSplitContext of(
            MongoReadOptions readOptions,
            MongoClient mongoClient,
            MongoNamespace namespace,
            BsonDocument collStats) {
        return new MongoSplitContext(
                readOptions,
                mongoClient,
                namespace,
                collStats.getBoolean(SHARDED_FIELD, BsonBoolean.FALSE).getValue(),
                collStats.getNumber(COUNT_FIELD, new BsonInt64(0)).longValue(),
                collStats.getNumber(SIZE_FIELD, new BsonInt64(0)).longValue(),
                collStats.getNumber(AVG_OBJ_SIZE_FIELD, new BsonInt64(0)).longValue());
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoReadOptions getReadOptions() {
        return readOptions;
    }

    public String getDatabaseName() {
        return namespace.getDatabaseName();
    }

    public String getCollectionName() {
        return namespace.getCollectionName();
    }

    public MongoNamespace getMongoNamespace() {
        return namespace;
    }

    public MongoCollection<BsonDocument> getMongoCollection() {
        return mongoClient
                .getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName())
                .withDocumentClass(BsonDocument.class);
    }

    public boolean isSharded() {
        return sharded;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }

    public long getAvgObjSize() {
        return avgObjSize;
    }
}
