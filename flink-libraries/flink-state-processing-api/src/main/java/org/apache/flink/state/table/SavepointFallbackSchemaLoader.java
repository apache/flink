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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CustomRestoreSerializerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.input.deserializer.MissingClassSerializerFactory;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.schema.StateSchemaExtractor;
import org.apache.flink.state.api.schema.StateSchemaInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared helpers for loading fallback {@link StateSchemaInfo} (original serializer snapshots) from
 * savepoint metadata, and for restoring a {@link TypeSerializer} from a snapshot in a way that
 * tolerates missing POJO/Avro classes.
 */
@Internal
final class SavepointFallbackSchemaLoader {

    private SavepointFallbackSchemaLoader() {}

    /**
     * Restores {@code snapshot} into a {@link TypeSerializer}, tolerating snapshots (e.g. {@code
     * PojoSerializerSnapshot}, {@code AvroSerializerSnapshot}) whose class is missing from the
     * classpath — including ones nested arbitrarily deep inside a composite snapshot (e.g. a {@code
     * ListSerializerSnapshot} wrapping a POJO element type), since {@code
     * CompositeTypeSerializerSnapshot#restoreSerializer()} eagerly restores all of its nested
     * serializers.
     *
     * <p>The {@link CustomRestoreSerializerFactory} registered here is consulted by the snapshot's
     * own {@code restoreSerializer()} whenever it encounters a missing class, at any nesting depth,
     * and builds a schema-only deserializer instead of throwing (see {@link
     * MissingClassSerializerFactory}). Snapshots whose class is present restore normally,
     * unaffected by the factory. The factory is cleared once this single, synchronous restore
     * completes so it cannot leak into unrelated work later scheduled on the same thread (e.g. a
     * reused planner thread).
     */
    static TypeSerializer<?> buildFallbackSerializer(TypeSerializerSnapshot<?> snapshot) {
        CustomRestoreSerializerFactory.set(MissingClassSerializerFactory::create);
        try {
            return snapshot.restoreSerializer();
        } finally {
            CustomRestoreSerializerFactory.remove();
        }
    }

    static StateSchemaInfo getSchema(String name, Map<String, StateSchemaInfo> fallbackSchemas) {
        StateSchemaInfo schema = fallbackSchemas.get(name);
        if (schema == null) {
            throw new IllegalStateException(
                    "No schema found for state '" + name + "' in savepoint.");
        }
        return schema;
    }

    /**
     * Reads the savepoint header and returns a map of state name → {@link StateSchemaInfo}, but
     * only when {@code anyNullTypeInfo} is {@code true}; otherwise returns an empty map to avoid
     * the overhead of reading the savepoint header.
     */
    static Map<String, StateSchemaInfo> loadFallbackSchemas(
            String statePath, OperatorIdentifier operatorIdentifier, boolean anyNullTypeInfo) {
        if (!anyNullTypeInfo) {
            return Collections.emptyMap();
        }

        try {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(statePath);
            OperatorState operatorState =
                    metadata.getOperatorStates().stream()
                            .filter(
                                    op ->
                                            op.getOperatorID()
                                                    .equals(operatorIdentifier.getOperatorId()))
                            .findFirst()
                            .orElse(null);
            if (operatorState == null) {
                return Collections.emptyMap();
            }

            Map<String, StateSchemaInfo> result = new HashMap<>();
            for (StateSchemaInfo info : StateSchemaExtractor.extractSchema(operatorState)) {
                result.put(info.stateName, info);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load fallback schemas from savepoint", e);
        }
    }
}
