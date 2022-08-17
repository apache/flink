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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoClient;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MAX_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MIN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.UNAUTHORIZED_ERROR;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.isValidShardedCollection;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.readChunks;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.readCollectionMetadata;

/**
 * Sharded Partitioner
 *
 * <p>Uses the chunks collection and partitions the collection based on the sharded collections
 * chunk ranges.
 *
 * <p>The following config collections' read privilege is required.
 *
 * <ul>
 *   <li>config.collections
 *   <li>config.chunks
 * </ul>
 */
@Internal
public class MongoShardedSplitter implements MongoSplitters.MongoSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoShardedSplitter.class);

    public static final MongoShardedSplitter INSTANCE = new MongoShardedSplitter();

    private MongoShardedSplitter() {}

    @Override
    public Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoNamespace namespace = splitContext.getMongoNamespace();
        MongoClient mongoClient = splitContext.getMongoClient();

        List<BsonDocument> chunks;
        BsonDocument collectionMetadata;
        try {
            collectionMetadata = readCollectionMetadata(mongoClient, namespace);
            if (!isValidShardedCollection(collectionMetadata)) {
                LOG.warn(
                        "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                        namespace);
                return MongoSampleSplitter.INSTANCE.split(splitContext);
            }
            chunks = readChunks(mongoClient, collectionMetadata);
        } catch (MongoQueryException e) {
            if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                LOG.warn(
                        "Unauthorized to read config.collections or config.chunks: {}, fallback to SampleSplitter.",
                        e.getErrorMessage());
            } else {
                LOG.warn(
                        "Read config.chunks collection failed: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            }
            return MongoSampleSplitter.INSTANCE.split(splitContext);
        }

        if (chunks.isEmpty()) {
            LOG.warn(
                    "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                    namespace);
            return MongoSampleSplitter.INSTANCE.split(splitContext);
        }

        List<MongoScanSourceSplit> sourceSplits = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument chunk = chunks.get(i);
            sourceSplits.add(
                    new MongoScanSourceSplit(
                            String.format("%s_%d", namespace, i),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            chunk.getDocument(MIN_FIELD),
                            chunk.getDocument(MAX_FIELD),
                            collectionMetadata.getDocument(KEY_FIELD)));
        }

        return sourceSplits;
    }
}
