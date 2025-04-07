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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

/**
 * Queryable state stream instance.
 *
 * @param <K> State key type
 * @param <V> State value type
 * @deprecated The Queryable State feature is deprecated since Flink 1.18, and will be removed in a
 *     future Flink major version.
 */
@PublicEvolving
@Deprecated
public class QueryableStateStream<K, V> {

    /** Name under which the state is queryable. */
    private final String queryableStateName;

    /** Key serializer for the state instance. */
    private final TypeSerializer<K> keySerializer;

    /** State descriptor for the state instance. */
    private final StateDescriptor<?, V> stateDescriptor;

    /**
     * Creates a queryable state stream.
     *
     * @param queryableStateName Name under which to publish the queryable state instance
     * @param stateDescriptor The state descriptor for the state instance
     * @param keySerializer Key serializer for the state instance
     */
    public QueryableStateStream(
            String queryableStateName,
            StateDescriptor<?, V> stateDescriptor,
            TypeSerializer<K> keySerializer) {

        this.queryableStateName =
                Preconditions.checkNotNull(queryableStateName, "Queryable state name");
        this.stateDescriptor = Preconditions.checkNotNull(stateDescriptor, "State Descriptor");
        this.keySerializer = Preconditions.checkNotNull(keySerializer, "Key serializer");
    }

    /**
     * Returns the name under which the state can be queried.
     *
     * @return Name under which state can be queried.
     */
    public String getQueryableStateName() {
        return queryableStateName;
    }

    /**
     * Returns the key serializer for the queryable state instance.
     *
     * @return Key serializer for the state instance.
     */
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /**
     * Returns the state descriptor for the queryable state instance.
     *
     * @return State descriptor for the state instance
     */
    public StateDescriptor<?, V> getStateDescriptor() {
        return stateDescriptor;
    }
}
