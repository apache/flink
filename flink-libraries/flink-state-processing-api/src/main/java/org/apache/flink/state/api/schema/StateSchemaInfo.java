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

package org.apache.flink.state.api.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

import javax.annotation.Nullable;

/**
 * Carries the schema information extracted from a single keyed state entry in a savepoint, without
 * requiring user POJO classes on the classpath.
 *
 * <p>Naming convention used throughout this package and {@code state.table}: "namespace" names the
 * raw concept as Flink's runtime state backend knows it ({@link #namespaceSnapshot}), "window" the
 * same concept once resolved to a user-facing table/column ({@link
 * KeyedStateSchemaInfo.StateEntryInfo#windowLogicalType}).
 */
@Internal
public final class StateSchemaInfo {

    /** Name of the state as registered by the operator. */
    public final String stateName;

    /** The kind of state (VALUE, LIST, MAP, etc.). */
    public final StateDescriptor.Type stateKind;

    /** Serializer snapshot for the key type. */
    public final TypeSerializerSnapshot<?> keySnapshot;

    /**
     * Serializer snapshot for the state value type. For MAP state this is the value type; use
     * {@link #mapKeySnapshot} for the map key type.
     */
    public final TypeSerializerSnapshot<?> valueSnapshot;

    /**
     * Serializer snapshot for the map key type. Non-null only for {@link StateDescriptor.Type#MAP}
     * state.
     */
    @Nullable public final TypeSerializerSnapshot<?> mapKeySnapshot;

    /**
     * Serializer snapshot for the state's namespace. Plain per-key state (the only kind the
     * savepoint/checkpoint table connector can read) is registered with {@code VoidNamespace}; a
     * different namespace (e.g. a window) means the state is scoped per-window rather than per-key,
     * and cannot be exposed as a flat keyed table.
     */
    @Nullable public final TypeSerializerSnapshot<?> namespaceSnapshot;

    public StateSchemaInfo(
            String stateName,
            StateDescriptor.Type stateKind,
            TypeSerializerSnapshot<?> keySnapshot,
            TypeSerializerSnapshot<?> valueSnapshot,
            @Nullable TypeSerializerSnapshot<?> mapKeySnapshot,
            @Nullable TypeSerializerSnapshot<?> namespaceSnapshot) {
        this.stateName = stateName;
        this.stateKind = stateKind;
        this.keySnapshot = keySnapshot;
        this.valueSnapshot = valueSnapshot;
        this.mapKeySnapshot = mapKeySnapshot;
        this.namespaceSnapshot = namespaceSnapshot;
    }
}
