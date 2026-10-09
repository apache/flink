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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;

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
     * <p>The returned serializer must be equal to this one, with the same hash code: a composite
     * serializer rebuilt around it then stays equal to the original composite.
     *
     * <p>This is called only on a serializer whose values a state backend migrates: the value
     * serializer of a value, reducing or aggregating state, the element serializer of a list state,
     * or the value serializer of a map state. It is never called on an arbitrary serializer
     * encountered while walking a type.
     */
    TypeSerializer<T> withStateSchemaEvolution();

    /**
     * Returns whether this serializer admits a backward-compatible schema change at restore, either
     * because it was returned by {@link #withStateSchemaEvolution()} or because it was derived from
     * such a serializer.
     */
    boolean isStateSchemaEvolutionEnabled();

    /**
     * Decorates a factory so that the serializer it produces for a state of the given type is armed
     * for schema evolution.
     *
     * <p>Only a caller whose backend migrates restored values through {@link
     * TypeSerializerSnapshot#migrate} may use this. A serializer produced by an undecorated factory
     * is never armed.
     */
    static SerializerFactory arming(SerializerFactory delegate, StateDescriptor.Type stateType) {
        // Not a lambda: SerializerFactory's single method is generic, which a lambda cannot
        // implement.
        return new SerializerFactory() {
            @Override
            public <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInformation) {
                return armStateValueSerializer(
                        delegate.createSerializer(typeInformation), stateType);
            }
        };
    }

    /**
     * Arms the serializer of a state of the given type, if it supports schema evolution at all.
     *
     * <p>What is armed is decided by the state type, because that is how a state backend chooses
     * the serializer it calls {@link TypeSerializerSnapshot#migrate} on: the serializer itself for
     * a value, reducing or aggregating state, the element serializer for a list state, and the
     * value serializer for a map state. Nothing deeper is armed, because a serializer below that
     * one is never handed to {@code migrate}, and arming it would let a compatibility check report
     * {@code compatibleAfterMigration} for bytes that nothing ever migrates. For the same reason a
     * value state whose value is a list or a map arms nothing inside it, and a map key is never
     * armed.
     *
     * <p>A list or map serializer whose nested serializer comes back unchanged is returned as the
     * same instance.
     *
     * <p>Operator state and broadcast state also register list and map descriptors but never
     * migrate values. Since a descriptor caches the serializer it was first initialized with, those
     * backends reject a serializer for which {@link #isArmed} holds.
     */
    @SuppressWarnings("unchecked")
    static <T> TypeSerializer<T> armStateValueSerializer(
            TypeSerializer<T> serializer, StateDescriptor.Type stateType) {
        switch (stateType) {
            case VALUE:
            case REDUCING:
            case AGGREGATING:
                return armLeaf(serializer);
            case LIST:
                if (serializer instanceof ListSerializer) {
                    ListSerializer<Object> list = (ListSerializer<Object>) serializer;
                    TypeSerializer<Object> element = list.getElementSerializer();
                    TypeSerializer<Object> armedElement = armLeaf(element);
                    return armedElement == element
                            ? serializer
                            : (TypeSerializer<T>) new ListSerializer<>(armedElement);
                }
                return serializer;
            case MAP:
                if (serializer instanceof MapSerializer) {
                    MapSerializer<Object, Object> map = (MapSerializer<Object, Object>) serializer;
                    TypeSerializer<Object> value = map.getValueSerializer();
                    TypeSerializer<Object> armedValue = armLeaf(value);
                    return armedValue == value
                            ? serializer
                            : (TypeSerializer<T>)
                                    new MapSerializer<>(map.getKeySerializer(), armedValue);
                }
                return serializer;
            default:
                return serializer;
        }
    }

    /**
     * Returns whether the given serializer carries a serializer armed for schema evolution where
     * {@link #armStateValueSerializer} arms one: the serializer itself, the element serializer of a
     * {@link ListSerializer}, or the value serializer of a {@link MapSerializer}. The check is by
     * serializer shape, not state type, so it also reports a list or map nested in a value state.
     */
    static boolean isArmed(TypeSerializer<?> serializer) {
        if (serializer instanceof ListSerializer) {
            return isArmedLeaf(((ListSerializer<?>) serializer).getElementSerializer());
        }
        if (serializer instanceof MapSerializer) {
            return isArmedLeaf(((MapSerializer<?, ?>) serializer).getValueSerializer());
        }
        return isArmedLeaf(serializer);
    }

    /** The only caller of {@link #withStateSchemaEvolution()}; it never descends. */
    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> armLeaf(TypeSerializer<T> serializer) {
        return serializer instanceof StateSchemaEvolvingSerializer
                ? ((StateSchemaEvolvingSerializer<T>) serializer).withStateSchemaEvolution()
                : serializer;
    }

    private static boolean isArmedLeaf(TypeSerializer<?> serializer) {
        return serializer instanceof StateSchemaEvolvingSerializer
                && ((StateSchemaEvolvingSerializer<?>) serializer).isStateSchemaEvolutionEnabled();
    }
}
