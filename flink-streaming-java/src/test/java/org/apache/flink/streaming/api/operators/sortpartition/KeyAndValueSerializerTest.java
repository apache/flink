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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyAndValueSerializer} in {@link KeyedSortPartitionOperator}. */
class KeyAndValueSerializerTest extends SerializerTestBase<Tuple2<byte[], Integer>> {

    private static final int DEFAULT_KEY_LENGTH = 4;

    private final IntSerializer valueSerializer = new IntSerializer();

    @Override
    protected TypeSerializer<Tuple2<byte[], Integer>> createSerializer() {
        return new KeyAndValueSerializer<>(valueSerializer, DEFAULT_KEY_LENGTH);
    }

    @Override
    protected int getLength() {
        return DEFAULT_KEY_LENGTH + valueSerializer.getLength();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Class<Tuple2<byte[], Integer>> getTypeClass() {
        return (Class<Tuple2<byte[], Integer>>) (Class) Tuple2.class;
    }

    @Override
    protected Tuple2<byte[], Integer>[] getTestData() {
        return SerializerComparatorTestData.getOrderedIntTestData();
    }

    @Override
    @Test
    public void testConfigSnapshotInstantiation() {
        assertThatThrownBy(() -> super.testConfigSnapshotInstantiation())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Override
    @Test
    public void testSnapshotConfigurationAndReconfigure() {
        assertThatThrownBy(() -> super.testSnapshotConfigurationAndReconfigure())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
