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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperatorV2.LeftTimeIndexKey;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperatorV2.LeftTimeIndexKeySerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link LeftTimeIndexKeySerializer}. */
class LeftTimeIndexKeySerializerTest extends SerializerTestBase<LeftTimeIndexKey> {

    @Override
    protected TypeSerializer<LeftTimeIndexKey> createSerializer() {
        return LeftTimeIndexKeySerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return 16;
    }

    @Override
    protected Class<LeftTimeIndexKey> getTypeClass() {
        return LeftTimeIndexKey.class;
    }

    @Override
    protected LeftTimeIndexKey[] getTestData() {
        Random rnd = new Random(874597969123412341L);
        long rndLong = rnd.nextLong();

        return new LeftTimeIndexKey[] {
            new LeftTimeIndexKey(0L, 0L),
            new LeftTimeIndexKey(1L, 0L),
            new LeftTimeIndexKey(-1L, 0L),
            new LeftTimeIndexKey(Long.MAX_VALUE, Long.MAX_VALUE),
            new LeftTimeIndexKey(Long.MIN_VALUE, 0L),
            new LeftTimeIndexKey(42L, 1L),
            new LeftTimeIndexKey(42L, 2L),
            new LeftTimeIndexKey(rndLong, 3L),
            new LeftTimeIndexKey(-rndLong, 4L)
        };
    }

    @Test
    void testSerializedByteOrderMatchesNumericOrder() throws IOException {
        List<LeftTimeIndexKey> keys = new ArrayList<>();
        long[] interestingValues = {
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            -42L,
            -1L,
            0L,
            1L,
            42L,
            Long.MAX_VALUE - 1,
            Long.MAX_VALUE
        };
        for (long timestamp : interestingValues) {
            for (long index : interestingValues) {
                keys.add(new LeftTimeIndexKey(timestamp, index));
            }
        }
        Random rnd = new Random(42);
        for (int i = 0; i < 100; i++) {
            keys.add(new LeftTimeIndexKey(rnd.nextLong(), rnd.nextLong()));
        }
        Collections.shuffle(keys, rnd);

        List<LeftTimeIndexKey> numericallySorted = new ArrayList<>(keys);
        numericallySorted.sort(
                Comparator.comparingLong(LeftTimeIndexKey::getTimestamp)
                        .thenComparingLong(LeftTimeIndexKey::getIndex));

        List<LeftTimeIndexKey> byteSorted = new ArrayList<>(keys);
        byteSorted.sort(
                Comparator.comparing(
                        LeftTimeIndexKeySerializerTest::serializeToBytes, Arrays::compareUnsigned));

        assertThat(byteSorted).containsExactlyElementsOf(numericallySorted);
    }

    private static byte[] serializeToBytes(LeftTimeIndexKey key) {
        DataOutputSerializer out = new DataOutputSerializer(16);
        try {
            LeftTimeIndexKeySerializer.INSTANCE.serialize(key, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.getCopyOfBuffer();
    }
}
