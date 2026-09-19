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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.UUID;

/**
 * A test for the {@link UuidComparator}.
 *
 * <p>The sorted data spans the signed-long boundary (a {@code 0x8000...} most-significant half
 * sorts after {@code 0x7FFF...}) to assert the unsigned big-endian ordering, which differs from
 * {@link UUID#compareTo(UUID)}.
 */
class UuidComparatorTest extends ComparatorTestBase<UUID> {

    @Override
    protected TypeComparator<UUID> createComparator(boolean ascending) {
        return new UuidComparator(ascending);
    }

    @Override
    protected TypeSerializer<UUID> createSerializer() {
        return new UuidSerializer();
    }

    @Override
    protected UUID[] getSortedTestData() {
        return new UUID[] {
            new UUID(0x0000000000000000L, 0x0000000000000000L),
            new UUID(0x0000000000000000L, 0x0000000000000001L),
            new UUID(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
            new UUID(0x0000000000000001L, 0x0000000000000000L),
            new UUID(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
            new UUID(0x8000000000000000L, 0x0000000000000000L),
            new UUID(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L),
            new UUID(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)
        };
    }
}
