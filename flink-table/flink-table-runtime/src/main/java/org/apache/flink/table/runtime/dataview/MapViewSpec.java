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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;

import javax.annotation.Nullable;

import java.util.Optional;

/** Specification for a {@link MapView}. */
@Internal
public class MapViewSpec extends DataViewSpec {

    private final boolean containsNullKey;

    private final @Nullable TypeSerializer<?> keySerializer;

    private final @Nullable TypeSerializer<?> valueSerializer;

    public MapViewSpec(String stateId, int fieldIndex, DataType dataType, boolean containsNullKey) {
        this(stateId, fieldIndex, dataType, containsNullKey, null, null);
    }

    @Deprecated
    public MapViewSpec(
            String stateId,
            int fieldIndex,
            DataType dataType,
            boolean containsNullKey,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> valueSerializer) {
        super(stateId, fieldIndex, dataType);
        this.containsNullKey = containsNullKey;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public DataType getKeyDataType() {
        final KeyValueDataType mapDataType = (KeyValueDataType) getDataType();
        return mapDataType.getKeyDataType();
    }

    public DataType getValueDataType() {
        final KeyValueDataType mapDataType = (KeyValueDataType) getDataType();
        return mapDataType.getValueDataType();
    }

    public Optional<TypeSerializer<?>> getKeySerializer() {
        return Optional.ofNullable(keySerializer);
    }

    public Optional<TypeSerializer<?>> getValueSerializer() {
        return Optional.ofNullable(valueSerializer);
    }

    public boolean containsNullKey() {
        return containsNullKey;
    }
}
