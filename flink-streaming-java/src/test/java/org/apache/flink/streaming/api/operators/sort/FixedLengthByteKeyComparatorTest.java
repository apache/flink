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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link FixedLengthByteKeyComparator}. */
public class FixedLengthByteKeyComparatorTest
        extends ComparatorTestBase<Tuple2<byte[], StreamRecord<Integer>>> {
    @Override
    protected Order[] getTestedOrder() {
        return new Order[] {Order.ASCENDING};
    }

    @Override
    protected TypeComparator<Tuple2<byte[], StreamRecord<Integer>>> createComparator(
            boolean ascending) {
        return new FixedLengthByteKeyComparator<>(new IntSerializer().getLength());
    }

    @Override
    protected TypeSerializer<Tuple2<byte[], StreamRecord<Integer>>> createSerializer() {
        IntSerializer intSerializer = new IntSerializer();
        return new KeyAndValueSerializer<>(intSerializer, intSerializer.getLength());
    }

    @Override
    protected void deepEquals(
            String message,
            Tuple2<byte[], StreamRecord<Integer>> should,
            Tuple2<byte[], StreamRecord<Integer>> is) {
        assertThat(message, should.f0, equalTo(is.f0));
        assertThat(message, should.f1, equalTo(is.f1));
    }

    @Override
    protected Tuple2<byte[], StreamRecord<Integer>>[] getSortedTestData() {
        return SerializerComparatorTestData.getOrderedIntTestData();
    }
}
