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
package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;

import org.junit.Assert;

public class LongPrimitiveArrayComparatorTest extends PrimitiveArrayComparatorTestBase<long[]> {
    public LongPrimitiveArrayComparatorTest() {
        super(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO);
    }

    @Override
    protected void deepEquals(String message, long[] should, long[] is) {
        Assert.assertArrayEquals(message, should, is);
    }

    @Override
    protected long[][] getSortedTestData() {
        return new long[][] {
            new long[] {-1, 0},
            new long[] {0, -1},
            new long[] {0, 0},
            new long[] {0, 1},
            new long[] {0, 1, 2},
            new long[] {2}
        };
    }
}
