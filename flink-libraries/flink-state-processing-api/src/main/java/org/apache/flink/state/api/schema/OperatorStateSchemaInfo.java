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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.state.table.SavepointConnectorOptions.StateReaderMode;

import javax.annotation.Nullable;

/**
 * Carries the schema information extracted from a single non-keyed (operator) state entry — {@code
 * ListState}, {@code UnionState}, or {@code BroadcastState} — in a savepoint, without requiring the
 * user POJO classes on the classpath.
 *
 * <p>Unlike {@link StateSchemaInfo}, there is no per-key serializer: non-keyed state has no key
 * concept. {@link #keySnapshot} is only present for {@link StateReaderMode#BROADCAST}, where it
 * represents the broadcast map's key.
 */
@Internal
public final class OperatorStateSchemaInfo {

    /** Name of the state as registered by the operator. */
    public final String stateName;

    public final StateReaderMode kind;

    /**
     * Serializer snapshot for the state's value type: the map's value type for {@link
     * StateReaderMode#BROADCAST}, the list element type for {@link StateReaderMode#LIST}/{@link
     * StateReaderMode#UNION}.
     */
    public final TypeSerializerSnapshot<?> valueSnapshot;

    /**
     * Broadcast map's key serializer snapshot; non-null only for {@link StateReaderMode#BROADCAST}.
     */
    @Nullable public final TypeSerializerSnapshot<?> keySnapshot;

    public OperatorStateSchemaInfo(
            String stateName,
            StateReaderMode kind,
            TypeSerializerSnapshot<?> valueSnapshot,
            @Nullable TypeSerializerSnapshot<?> keySnapshot) {
        this.stateName = stateName;
        this.kind = kind;
        this.valueSnapshot = valueSnapshot;
        this.keySnapshot = keySnapshot;
    }
}
