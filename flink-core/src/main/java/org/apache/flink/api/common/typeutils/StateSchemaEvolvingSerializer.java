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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A {@link TypeSerializer} that can admit a backward-compatible change of the schema of the values
 * it serializes, provided the state it belongs to will actually have those values migrated.
 *
 * <p>Implementations are inert by default: a serializer only starts admitting schema changes once
 * {@link #withStateSchemaEvolution()} has been called on it, and only if the job configuration
 * opted in.
 *
 * @param <T> the type of the serialized values
 */
@Internal
public interface StateSchemaEvolvingSerializer<T> {

    /**
     * Returns a serializer that admits a backward-compatible schema change and migrates the stored
     * values when state is restored, or {@code this} if the job configuration did not opt in.
     *
     * <p>This is called only on a state's own value serializer, never on an arbitrary serializer
     * encountered while walking a type.
     */
    TypeSerializer<T> withStateSchemaEvolution();

    /**
     * Decorates a factory so that the serializer it produces for a state value is armed for schema
     * evolution.
     *
     * <p>Only a caller whose backend migrates restored values through {@link
     * TypeSerializerSnapshot#migrate} may use this. A caller that does not decorate its factory
     * keeps today's behavior unchanged.
     */
    static SerializerFactory arming(SerializerFactory delegate) {
        // Not a lambda: SerializerFactory's single method is generic, which a lambda cannot
        // implement.
        return new SerializerFactory() {
            @Override
            public <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInformation) {
                return armStateValueSerializer(delegate.createSerializer(typeInformation));
            }
        };
    }

    /**
     * Arms the serializer a state holds for its values, if it supports schema evolution at all.
     *
     * <p>The serializer passed in is armed, and nothing below it: there is no descent into a
     * composite, and no recursion. A serializer nested below the state value is not reached by
     * {@link TypeSerializerSnapshot#migrate}, so arming one would let a compatibility check report
     * {@code compatibleAfterMigration} for bytes that nothing ever migrates.
     *
     * <p>The serializers armed here are therefore a subset of those {@code
     * TtlAwareSerializer#wrapTtlAwareSerializer} descends into, which are the ones some backend
     * calls {@code migrate} on. Being a subset is what keeps this sound. Widening the descent to
     * close the gap is only safe once every caller registering the widened shape is known to reach
     * such a backend, which is not true of the seam as it stands: operator state and broadcast
     * state register list and map descriptors through it and never migrate.
     */
    @SuppressWarnings("unchecked")
    static <T> TypeSerializer<T> armStateValueSerializer(TypeSerializer<T> serializer) {
        return serializer instanceof StateSchemaEvolvingSerializer
                ? ((StateSchemaEvolvingSerializer<T>) serializer).withStateSchemaEvolution()
                : serializer;
    }
}
