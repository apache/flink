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
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.common.utils.MongoUtils;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ERROR_MESSAGE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoUtils.isCommandSucceed;

/** To split collections of MongoDB to {@link MongoSourceSplit}s. */
@Internal
public class MongoSplitters implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoSplitters.class);

    private final MongoReadOptions readOptions;
    private final boolean limitPushedDown;
    private final MongoClient mongoClient;

    public MongoSplitters(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            boolean limitPushedDown) {
        this.readOptions = readOptions;
        this.limitPushedDown = limitPushedDown;
        this.mongoClient = MongoClients.create(connectionOptions.getUri());
    }

    public Collection<MongoScanSourceSplit> split(MongoNamespace namespace) {
        BsonDocument collStats = MongoUtils.collStats(mongoClient, namespace);
        if (!isCommandSucceed(collStats)) {
            LOG.error(
                    "Execute command collStats failed: {}",
                    collStats.getString(ERROR_MESSAGE_FIELD));
            throw new IllegalStateException(String.format("Collection not found %s", namespace));
        }

        MongoSplitContext splitContext =
                MongoSplitContext.of(readOptions, mongoClient, namespace, collStats);

        if (limitPushedDown) {
            LOG.info("Limit {} is applied, using single splitter", limitPushedDown);
            return MongoSingleSplitter.INSTANCE.split(splitContext);
        }

        PartitionStrategy strategy = readOptions.getPartitionStrategy();
        switch (strategy) {
            case SINGLE:
                return MongoSingleSplitter.INSTANCE.split(splitContext);
            case SAMPLE:
                return MongoSampleSplitter.INSTANCE.split(splitContext);
            case SPLIT_VECTOR:
                return MongoSplitVectorSplitter.INSTANCE.split(splitContext);
            case SHARDED:
                return MongoShardedSplitter.INSTANCE.split(splitContext);
            case DEFAULT:
            default:
                return splitContext.isSharded()
                        ? MongoShardedSplitter.INSTANCE.split(splitContext)
                        : MongoSplitVectorSplitter.INSTANCE.split(splitContext);
        }
    }

    @Override
    public void close() throws IOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    /** Mongo Splitter to split a collection into multiple splits. */
    public interface MongoSplitter {
        Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext);
    }
}
