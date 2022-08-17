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

package org.apache.flink.connector.mongodb.common.utils;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.conversions.Bson;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DROPPED_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MAX_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MIN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.NAMESPACE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.OK_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SHARD_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.UUID_FIELD;

/** A util class with some helper method for MongoDB commands. */
public class MongoUtils {

    public static final int UNAUTHORIZED_ERROR = 13;

    public static final String COLL_STATS_COMMAND = "collStats";
    public static final String SPLIT_VECTOR_COMMAND = "splitVector";
    public static final String KEY_PATTERN_OPTION = "keyPattern";
    public static final String MAX_CHUNK_SIZE_OPTION = "maxChunkSize";

    public static final String CONFIG_DATABASE = "config";
    public static final String COLLECTIONS_COLLECTION = "collections";
    public static final String CHUNKS_COLLECTION = "chunks";

    private MongoUtils() {}

    public static BsonDocument collStats(MongoClient mongoClient, MongoNamespace namespace) {
        BsonDocument collStatsCommand =
                new BsonDocument(COLL_STATS_COMMAND, new BsonString(namespace.getCollectionName()));
        return mongoClient
                .getDatabase(namespace.getDatabaseName())
                .runCommand(collStatsCommand, BsonDocument.class);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            MongoNamespace namespace,
            BsonDocument keyPattern,
            int maxChunkSizeMB) {
        return splitVector(mongoClient, namespace, keyPattern, maxChunkSizeMB, null, null);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            MongoNamespace namespace,
            BsonDocument keyPattern,
            int maxChunkSizeMB,
            @Nullable BsonDocument min,
            @Nullable BsonDocument max) {
        BsonDocument splitVectorCommand =
                new BsonDocument(SPLIT_VECTOR_COMMAND, new BsonString(namespace.getFullName()))
                        .append(KEY_PATTERN_OPTION, keyPattern)
                        .append(MAX_CHUNK_SIZE_OPTION, new BsonInt32(maxChunkSizeMB));
        Optional.ofNullable(min).ifPresent(v -> splitVectorCommand.append(MIN_FIELD, v));
        Optional.ofNullable(max).ifPresent(v -> splitVectorCommand.append(MAX_FIELD, v));
        return mongoClient
                .getDatabase(namespace.getDatabaseName())
                .runCommand(splitVectorCommand, BsonDocument.class);
    }

    @Nullable
    public static BsonDocument readCollectionMetadata(
            MongoClient mongoClient, MongoNamespace namespace) {
        MongoCollection<BsonDocument> collection =
                mongoClient
                        .getDatabase(CONFIG_DATABASE)
                        .getCollection(COLLECTIONS_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        return collection
                .find(eq(ID_FIELD, namespace.getFullName()))
                .projection(include(ID_FIELD, UUID_FIELD, DROPPED_FIELD, KEY_FIELD))
                .first();
    }

    public static boolean isValidShardedCollection(BsonDocument collectionMetadata) {
        return collectionMetadata != null
                && !collectionMetadata.getBoolean(DROPPED_FIELD, BsonBoolean.FALSE).getValue();
    }

    public static List<BsonDocument> readChunks(
            MongoClient mongoClient, BsonDocument collectionMetadata) {
        MongoCollection<BsonDocument> chunks =
                mongoClient
                        .getDatabase(CONFIG_DATABASE)
                        .getCollection(CHUNKS_COLLECTION)
                        .withDocumentClass(BsonDocument.class);

        Bson filter =
                or(
                        new BsonDocument(NAMESPACE_FIELD, collectionMetadata.get(ID_FIELD)),
                        // MongoDB 4.9.0 removed ns field of config.chunks collection, using
                        // collection's uuid instead.
                        // See: https://jira.mongodb.org/browse/SERVER-53105
                        new BsonDocument(UUID_FIELD, collectionMetadata.get(UUID_FIELD)));

        return chunks.find(filter)
                .projection(include(MIN_FIELD, MAX_FIELD, SHARD_FIELD))
                .sort(ascending(MIN_FIELD))
                .into(new ArrayList<>());
    }

    public static boolean isCommandSucceed(BsonDocument commandResult) {
        return commandResult != null
                && commandResult.getDouble(OK_FIELD) != null
                && commandResult.getDouble(OK_FIELD).doubleValue() > 0.0d;
    }

    public static Bson project(List<String> projectedFields) {
        if (projectedFields.contains(ID_FIELD)) {
            return include(projectedFields);
        } else {
            // Creates a projection that excludes the _id field.
            // This suppresses the automatic inclusion of _id that is default.
            return fields(include(projectedFields), excludeId());
        }
    }
}
