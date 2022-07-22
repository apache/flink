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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortUtil}. */
public class SortUtilTest {

    @Test
    public void testNormalizedKey() {
        int len = 10;
        Random random = new Random();
        MemorySegment[] segments = new MemorySegment[len];
        MemorySegment[] compareSegs = new MemorySegment[len];
        for (int i = 0; i < len; i++) {
            segments[i] = MemorySegmentFactory.allocateUnpooledSegment(20);
            compareSegs[i] = MemorySegmentFactory.allocateUnpooledSegment(20);
        }

        {
            SortUtil.minNormalizedKey(segments[0], 0, 20);
            SortUtil.maxNormalizedKey(segments[1], 0, 20);
            for (int i = 0; i < len; i++) {
                byte[] rndBytes = new byte[20];
                random.nextBytes(rndBytes);
                segments[2].put(0, rndBytes);
                assertThat(segments[0].compare(segments[2], 0, 0, 20)).isLessThanOrEqualTo(0);
                assertThat(segments[1].compare(segments[2], 0, 0, 20)).isGreaterThanOrEqualTo(0);
            }
        }

        {
            DecimalData[] arr = new DecimalData[len];
            for (int i = 0; i < len; i++) {
                arr[i] = DecimalData.fromBigDecimal(new BigDecimal(random.nextInt()), 18, 0);
                SortUtil.putDecimalNormalizedKey(arr[i], segments[i], 0, 8);
            }
            Arrays.sort(arr, DecimalData::compareTo);
            for (int i = 0; i < len; i++) {
                SortUtil.putDecimalNormalizedKey(arr[i], compareSegs[i], 0, 8);
            }

            Arrays.sort(segments, (o1, o2) -> o1.compare(o2, 0, 0, 8));
            for (int i = 0; i < len; i++) {
                assertThat(compareSegs[i].equalTo(segments[i], 0, 0, 8)).isTrue();
            }
        }

        {
            Float[] arr = new Float[len];
            for (int i = 0; i < len; i++) {
                arr[i] = random.nextFloat();
                SortUtil.putFloatNormalizedKey(arr[i], segments[i], 0, 4);
            }

            Arrays.sort(arr, Float::compareTo);
            for (int i = 0; i < len; i++) {
                SortUtil.putFloatNormalizedKey(arr[i], compareSegs[i], 0, 4);
            }

            Arrays.sort(segments, (o1, o2) -> o1.compare(o2, 0, 0, 4));
            for (int i = 0; i < len; i++) {
                assertThat(compareSegs[i].equalTo(segments[i], 0, 0, 4)).isTrue();
            }
        }

        {
            Double[] arr = new Double[len];
            for (int i = 0; i < len; i++) {
                arr[i] = random.nextDouble();
                SortUtil.putDoubleNormalizedKey(arr[i], segments[i], 0, 8);
            }

            Arrays.sort(arr, Double::compareTo);
            for (int i = 0; i < len; i++) {
                SortUtil.putDoubleNormalizedKey(arr[i], compareSegs[i], 0, 8);
            }

            Arrays.sort(segments, (o1, o2) -> o1.compare(o2, 0, 0, 8));
            for (int i = 0; i < len; i++) {
                assertThat(compareSegs[i].equalTo(segments[i], 0, 0, 8)).isTrue();
            }
        }

        {
            BinaryStringData[] arr = new BinaryStringData[len];
            for (int i = 0; i < len; i++) {
                arr[i] = BinaryStringData.fromString(String.valueOf(random.nextLong()));
                SortUtil.putStringNormalizedKey(arr[i], segments[i], 0, 8);
            }

            Arrays.sort(arr, StringData::compareTo);
            for (int i = 0; i < len; i++) {
                SortUtil.putStringNormalizedKey(arr[i], compareSegs[i], 0, 8);
            }

            Arrays.sort(segments, (o1, o2) -> o1.compare(o2, 0, 0, 8));
            for (int i = 0; i < len; i++) {
                assertThat(compareSegs[i].equalTo(segments[i], 0, 0, 8)).isTrue();
            }
        }
    }
}
