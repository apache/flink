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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Represents a MongoDB <b>Replace One</b> operation.
 *
 * <p>The Replace One operation Replaces a single document within the collection based on the
 * filter. To ensure the MongoDB Sink at least one guarantee, the <b>upsert</b> flag is set to true
 * by default.
 */
@PublicEvolving
public final class MongoDbReplaceOneOperation extends MongoDbWriteOperation {

    /* The selection criteria for the update. Represented as a Map of String to Object values. */
    private final Map<String, Object> filter;

    /* The replacement document. Represented as a Map of String to Object values. */
    private final Map<String, Object> replacement;

    /* Replace options. */
    private final MongoDbReplaceOptions replaceOptions;

    public MongoDbReplaceOneOperation(
            Map<String, Object> filter,
            Map<String, Object> replacement,
            MongoDbReplaceOptions replaceOptions) {
        this.filter = Preconditions.checkNotNull(filter);
        this.replacement = Preconditions.checkNotNull(replacement);
        this.replaceOptions = Preconditions.checkNotNull(replaceOptions);
    }

    public MongoDbReplaceOneOperation(Map<String, Object> filter, Map<String, Object> replacement) {
        this(filter, replacement, new MongoDbReplaceOptions());
    }

    public Map<String, Object> getFilter() {
        return filter;
    }

    public Map<String, Object> getReplacement() {
        return replacement;
    }

    public MongoDbReplaceOptions getReplaceOptions() {
        return replaceOptions;
    }
}
