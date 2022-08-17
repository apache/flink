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

import org.bson.BsonDocument;

import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;

/** Mongo Splitter that splits MongoDB collection as a single split. */
@Internal
public class MongoSingleSplitter implements MongoSplitters.MongoSplitter {

    public static final MongoSingleSplitter INSTANCE = new MongoSingleSplitter();

    private MongoSingleSplitter() {}

    @Override
    public Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoScanSourceSplit singleSplit =
                new MongoScanSourceSplit(
                        splitContext.getMongoNamespace().getFullName(),
                        splitContext.getDatabaseName(),
                        splitContext.getCollectionName(),
                        new BsonDocument(ID_FIELD, BSON_MIN_KEY),
                        new BsonDocument(ID_FIELD, BSON_MAX_KEY),
                        ID_HINT);

        return singletonList(singleSplit);
    }
}
