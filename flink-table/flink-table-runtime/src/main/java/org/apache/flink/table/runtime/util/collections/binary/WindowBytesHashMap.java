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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.WindowKey;

/**
 * A binary map in the structure like {@code Map<WindowKey, BinaryRowData>}.
 *
 * @see AbstractBytesHashMap for more information about the binary layout.
 */
public final class WindowBytesHashMap extends AbstractBytesHashMap<WindowKey> {

    public WindowBytesHashMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<RowData> keySer,
            int valueArity) {
        super(owner, memoryManager, memorySize, new WindowKeySerializer(keySer), valueArity);
    }
}
