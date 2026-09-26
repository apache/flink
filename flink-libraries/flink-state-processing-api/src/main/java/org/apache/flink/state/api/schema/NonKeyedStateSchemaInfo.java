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
import org.apache.flink.state.table.SavepointConnectorOptions.StateReaderMode;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;

/**
 * Schema information for all non-keyed (operator) states of a single operator — {@code ListState},
 * {@code UnionState}, and {@code BroadcastState} — extracted from a savepoint without requiring the
 * user POJO classes on the classpath.
 *
 * <p>Unlike {@link KeyedStateSchemaInfo}, there is no shared key type across entries: operator
 * state has no per-key partitioning, and only {@link StateReaderMode#BROADCAST} entries carry a
 * (per-entry) map key type.
 */
@Internal
public final class NonKeyedStateSchemaInfo {

    /** State names to entry info, in savepoint registration order. */
    public final LinkedHashMap<String, StateEntryInfo> stateSchemas;

    public NonKeyedStateSchemaInfo(LinkedHashMap<String, StateEntryInfo> stateSchemas) {
        this.stateSchemas = stateSchemas;
    }

    /** Schema information for one non-keyed state entry. */
    public static final class StateEntryInfo {

        public final StateReaderMode kind;

        /** Logical type of the state's value (list element type, or broadcast map's value type). */
        public final LogicalType valueLogicalType;

        /** Broadcast map's key type; non-null only for {@link StateReaderMode#BROADCAST}. */
        @Nullable public final LogicalType mapKeyLogicalType;

        public StateEntryInfo(
                StateReaderMode kind,
                LogicalType valueLogicalType,
                @Nullable LogicalType mapKeyLogicalType) {
            this.kind = kind;
            this.valueLogicalType = valueLogicalType;
            this.mapKeyLogicalType = mapKeyLogicalType;
        }
    }
}
