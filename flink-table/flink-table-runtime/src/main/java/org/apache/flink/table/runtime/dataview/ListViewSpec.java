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
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Optional;

/** Specification for a {@link ListView}. */
@Internal
public class ListViewSpec extends DataViewSpec {

    private final @Nullable TypeSerializer<?> elementSerializer;

    public ListViewSpec(String stateId, int fieldIndex, DataType dataType) {
        this(stateId, fieldIndex, dataType, null);
    }

    @Deprecated
    public ListViewSpec(
            String stateId,
            int fieldIndex,
            DataType dataType,
            TypeSerializer<?> elementSerializer) {
        super(stateId, fieldIndex, dataType);
        this.elementSerializer = elementSerializer;
    }

    public DataType getElementDataType() {
        final CollectionDataType arrayDataType = (CollectionDataType) getDataType();
        return arrayDataType.getElementDataType();
    }

    public Optional<TypeSerializer<?>> getElementSerializer() {
        return Optional.ofNullable(elementSerializer);
    }
}
