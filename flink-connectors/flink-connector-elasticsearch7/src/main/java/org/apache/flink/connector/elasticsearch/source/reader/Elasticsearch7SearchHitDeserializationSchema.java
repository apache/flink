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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.Serializable;

/**
 * A schema bridge for deserializing Elasticsearch's {@link SearchHit} into a flink managed
 * instance.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
public interface Elasticsearch7SearchHitDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    @PublicEvolving
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /** Deserializes the search hit. */
    @PublicEvolving
    void deserialize(SearchHit record, Collector<T> out) throws IOException;
}
