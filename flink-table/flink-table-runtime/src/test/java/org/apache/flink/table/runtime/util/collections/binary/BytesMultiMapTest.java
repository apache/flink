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

/** Verify the correctness of {@link BytesMultiMap}. */
public class BytesMultiMapTest extends BytesMultiMapTestBase<BinaryRowData> {

    public BytesMultiMapTest() {
        super(new BinaryRowDataSerializer(KEY_TYPES.length));
    }

    @Override
    public AbstractBytesMultiMap<BinaryRowData> createBytesMultiMap(
            MemoryManager memoryManager,
            int memorySize,
            LogicalType[] keyTypes,
            LogicalType[] valueTypes) {
        return new BytesMultiMap(this, memoryManager, memorySize, keyTypes, valueTypes);
    }

    @Override
    public BinaryRowData[] generateRandomKeys(int num) {
        return getRandomizedInputs(num);
    }
}
