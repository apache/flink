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
import org.apache.flink.state.table.SavepointConnectorOptions;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;

/**
 * Schema information for all keyed states of a single operator, extracted from a savepoint without
 * requiring user POJO classes on the classpath.
 */
@Internal
public final class KeyedStateSchemaInfo {

    /** The logical type of the keyed state backend key (e.g., BigIntType for Long keys). */
    public final LogicalType keyType;

    /**
     * Ordered map of registered state names to their entry information. Ordered by the registration
     * order found in the savepoint.
     */
    public final LinkedHashMap<String, StateEntryInfo> stateSchemas;

    public KeyedStateSchemaInfo(
            LogicalType keyType, LinkedHashMap<String, StateEntryInfo> stateSchemas) {
        this.keyType = keyType;
        this.stateSchemas = stateSchemas;
    }

    /** Schema information for one keyed state entry. */
    public static final class StateEntryInfo {

        /** VALUE, LIST, or MAP. */
        public final SavepointConnectorOptions.StateType stateType;

        /**
         * The SQL column logical type.
         *
         * <ul>
         *   <li>VALUE&lt;Long&gt;: BigIntType
         *   <li>VALUE&lt;POJO&gt;: RowType (field names + types from the serializer snapshot)
         *   <li>LIST&lt;Long&gt;: ArrayType(BigIntType)
         *   <li>MAP&lt;Long,Long&gt;: MapType(BigIntType, BigIntType)
         * </ul>
         */
        public final LogicalType logicalType;

        /**
         * The resolved {@link LogicalType} of the state's namespace, or {@code null} if the state
         * is plain per-key state (registered under {@code VoidNamespace}). Non-null means the state
         * is scoped by some other namespace (e.g. a window), resolved generically by {@link
         * SerializerSnapshotToLogicalTypeConverter} rather than as a fixed {@code
         * TimeWindow}-shaped type.
         *
         * <p>Named "window" rather than "namespace" because this is the user-facing,
         * post-conversion form; see {@link StateSchemaInfo} for the raw/resolved naming convention.
         */
        @Nullable public final LogicalType windowLogicalType;

        public StateEntryInfo(
                SavepointConnectorOptions.StateType stateType,
                LogicalType logicalType,
                @Nullable LogicalType windowLogicalType) {
            this.stateType = stateType;
            this.logicalType = logicalType;
            this.windowLogicalType = windowLogicalType;
        }
    }
}
