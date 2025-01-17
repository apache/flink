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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A reference to a serializer. This also provides functions for lazy initialization.
 * Package-private for internal use only.
 *
 * @param <T> the type for serialization.
 */
@Internal
class StateSerializerReference<T> extends AtomicReference<TypeSerializer<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(StateSerializerReference.class);

    /**
     * The type information describing the value type. Only used to if the serializer is created
     * lazily.
     */
    @Nullable private final TypeInformation<T> typeInfo;

    public StateSerializerReference(TypeInformation<T> typeInfo) {
        super(null);
        this.typeInfo = checkNotNull(typeInfo, "type information must not be null");
    }

    public StateSerializerReference(TypeSerializer<T> typeSerializer) {
        super(checkNotNull(typeSerializer, "type serializer must not be null"));
        this.typeInfo = null;
    }

    public TypeInformation<T> getTypeInformation() {
        return typeInfo;
    }

    /**
     * Checks whether the serializer has been initialized. Serializer initialization is lazy, to
     * allow parametrization of serializers with an {@link ExecutionConfig} via {@link
     * #initializeUnlessSet(ExecutionConfig)}.
     *
     * @return True if the serializers have been initialized, false otherwise.
     */
    public boolean isInitialized() {
        return get() != null;
    }

    /**
     * Initializes the serializer, unless it has been initialized before.
     *
     * @param executionConfig The execution config to use when creating the serializer.
     */
    public void initializeUnlessSet(ExecutionConfig executionConfig) {
        initializeUnlessSet(
                new SerializerFactory() {
                    @Override
                    public <T> TypeSerializer<T> createSerializer(
                            TypeInformation<T> typeInformation) {
                        return typeInformation.createSerializer(
                                executionConfig == null
                                        ? null
                                        : executionConfig.getSerializerConfig());
                    }
                });
    }

    @Internal
    public void initializeUnlessSet(SerializerFactory serializerFactory) {
        if (get() == null) {
            checkState(typeInfo != null, "type information should not be null.");
            // try to instantiate and set the serializer
            TypeSerializer<T> serializer = serializerFactory.createSerializer(typeInfo);
            // use cas to assure the singleton
            if (!compareAndSet(null, serializer)) {
                LOG.debug("Someone else beat us at initializing the serializer.");
            }
        }
    }
}
