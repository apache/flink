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

package org.apache.flink.core.memory;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Verifies correct accesses with regards to endianness in {@link MemorySegment} (in both heap and
 * off-heap modes).
 */
class EndiannessAccessChecks {

    @Test
    void testOnHeapSegment() {
        testBigAndLittleEndianAccessUnaligned(MemorySegmentFactory.wrap(new byte[11111]));
    }

    @Test
    void testOffHeapSegment() {
        testBigAndLittleEndianAccessUnaligned(
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(11111));
    }

    @Test
    void testOffHeapUnsafeSegment() {
        testBigAndLittleEndianAccessUnaligned(
                MemorySegmentFactory.allocateOffHeapUnsafeMemory(11111));
    }

    private void testBigAndLittleEndianAccessUnaligned(MemorySegment segment) {
        final Random rnd = new Random();

        // longs
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                long val = rnd.nextLong();
                int pos = rnd.nextInt(segment.size() - 7);

                segment.putLongLittleEndian(pos, val);
                long r = segment.getLongBigEndian(pos);
                assertThat(Long.reverseBytes(r)).isEqualTo(val);

                segment.putLongBigEndian(pos, val);
                r = segment.getLongLittleEndian(pos);
                assertThat(Long.reverseBytes(r)).isEqualTo(val);
            }
        }

        // ints
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                int val = rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 3);

                segment.putIntLittleEndian(pos, val);
                int r = segment.getIntBigEndian(pos);
                assertThat(Integer.reverseBytes(r)).isEqualTo(val);

                segment.putIntBigEndian(pos, val);
                r = segment.getIntLittleEndian(pos);
                assertThat(Integer.reverseBytes(r)).isEqualTo(val);
            }
        }

        // shorts
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                short val = (short) rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 1);

                segment.putShortLittleEndian(pos, val);
                short r = segment.getShortBigEndian(pos);
                assertThat(Short.reverseBytes(r)).isEqualTo(val);

                segment.putShortBigEndian(pos, val);
                r = segment.getShortLittleEndian(pos);
                assertThat(Short.reverseBytes(r)).isEqualTo(val);
            }
        }

        // chars
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                char val = (char) rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 1);

                segment.putCharLittleEndian(pos, val);
                char r = segment.getCharBigEndian(pos);
                assertThat(Character.reverseBytes(r)).isEqualTo(val);

                segment.putCharBigEndian(pos, val);
                r = segment.getCharLittleEndian(pos);
                assertThat(Character.reverseBytes(r)).isEqualTo(val);
            }
        }

        // floats
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                float val = rnd.nextFloat();
                int pos = rnd.nextInt(segment.size() - 3);

                segment.putFloatLittleEndian(pos, val);
                float r = segment.getFloatBigEndian(pos);
                float reversed =
                        Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(r)));
                assertThat(reversed).isCloseTo(val, within(0.0f));

                segment.putFloatBigEndian(pos, val);
                r = segment.getFloatLittleEndian(pos);
                reversed = Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(r)));
                assertThat(reversed).isCloseTo(val, within(0.0f));
            }
        }

        // doubles
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                double val = rnd.nextDouble();
                int pos = rnd.nextInt(segment.size() - 7);

                segment.putDoubleLittleEndian(pos, val);
                double r = segment.getDoubleBigEndian(pos);
                double reversed =
                        Double.longBitsToDouble(Long.reverseBytes(Double.doubleToRawLongBits(r)));
                assertThat(reversed).isCloseTo(val, within(0.0d));

                segment.putDoubleBigEndian(pos, val);
                r = segment.getDoubleLittleEndian(pos);
                reversed =
                        Double.longBitsToDouble(Long.reverseBytes(Double.doubleToRawLongBits(r)));
                assertThat(reversed).isCloseTo(val, within(0.0d));
            }
        }
    }
}
