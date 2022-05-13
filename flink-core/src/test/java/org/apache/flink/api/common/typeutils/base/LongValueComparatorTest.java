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
import org.apache.flink.types.LongValue;

import java.util.Random;

public class LongValueComparatorTest extends ComparatorTestBase<LongValue> {

    @Override
    protected TypeComparator<LongValue> createComparator(boolean ascending) {
        return new LongValueComparator(ascending);
    }

    @Override
    protected TypeSerializer<LongValue> createSerializer() {
        return new LongValueSerializer();
    }

    @Override
    protected LongValue[] getSortedTestData() {
        Random rnd = new Random(874597969123412338L);
        long rndLong = rnd.nextLong();
        if (rndLong < 0) {
            rndLong = -rndLong;
        }
        if (rndLong == Long.MAX_VALUE) {
            rndLong -= 3;
        }
        if (rndLong <= 2) {
            rndLong += 3;
        }
        return new LongValue[] {
            new LongValue(Long.MIN_VALUE),
            new LongValue(-rndLong),
            new LongValue(-1L),
            new LongValue(0L),
            new LongValue(1L),
            new LongValue(2L),
            new LongValue(rndLong),
            new LongValue(Long.MAX_VALUE)
        };
    }
}
