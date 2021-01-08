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
import org.apache.flink.table.runtime.typeutils.WindowKeySerializer;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Random;

/** Verify the correctness of {@link WindowBytesMultiMap}. */
public class WindowBytesMultiMapTest extends BytesMultiMapTestBase<WindowKey> {

    public WindowBytesMultiMapTest() {
        super(new WindowKeySerializer(KEY_TYPES.length));
    }

    @Override
    public AbstractBytesMultiMap<WindowKey> createBytesMultiMap(
            MemoryManager memoryManager,
            int memorySize,
            LogicalType[] keyTypes,
            LogicalType[] valueTypes) {
        return new WindowBytesMultiMap(this, memoryManager, memorySize, keyTypes, valueTypes);
    }

    @Override
    public WindowKey[] generateRandomKeys(int num) {
        final Random rnd = new Random(RANDOM_SEED);
        BinaryRowData[] keys = getRandomizedInputs(num, rnd, true);
        WindowKey[] windowKeys = new WindowKey[num];
        for (int i = 0; i < num; i++) {
            windowKeys[i] = new WindowKey(rnd.nextLong(), keys[i]);
        }
        return windowKeys;
    }
}
