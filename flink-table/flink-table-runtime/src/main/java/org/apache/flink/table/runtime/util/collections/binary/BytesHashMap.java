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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A binary map in the structure like {@code Map<BinaryRowData, BinaryRowData>}.
 *
 * <p>{@code BytesHashMap} is influenced by Apache Spark BytesToBytesMap.
 *
 * @see AbstractBytesHashMap for more information about the binary layout.
 */
public final class BytesHashMap extends AbstractBytesHashMap<BinaryRowData> {

    public BytesHashMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            LogicalType[] keyTypes,
            LogicalType[] valueTypes) {
        super(
                owner,
                memoryManager,
                memorySize,
                new BinaryRowDataSerializer(keyTypes.length),
                valueTypes);
        checkArgument(keyTypes.length > 0);
    }
}
