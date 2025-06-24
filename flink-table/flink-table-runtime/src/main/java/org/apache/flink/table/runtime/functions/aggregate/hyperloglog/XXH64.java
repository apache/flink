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

package org.apache.flink.table.runtime.functions.aggregate.hyperloglog;

import org.apache.flink.core.memory.MemorySegment;

/**
 * xxHash64. A high quality and fast 64 bit hash code by Yann Colet and Mathias Westerdahl. The
 * class below is modelled like its Murmur3_x86_32 cousin.
 *
 * <p>This was largely based on the following (original) C and Java implementations:
 * https://github.com/Cyan4973/xxHash/blob/master/xxhash.c
 * https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/src/main/java/net/openhft/hashing/XxHash_r39.java
 * https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/XxHash64.java
 */
public final class XXH64 {

    public static final long DEFAULT_SEED = 42L;

    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    public static long hashInt(int input, long seed) {
        long hash = seed + PRIME64_5 + 4L;
        hash ^= (input & 0xFFFFFFFFL) * PRIME64_1;
        hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
        return fmix(hash);
    }

    public static long hashLong(long input, long seed) {
        long hash = seed + PRIME64_5 + 8L;
        hash ^= Long.rotateLeft(input * PRIME64_2, 31) * PRIME64_1;
        hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
        return fmix(hash);
    }

    public static long hashUnsafeBytes(MemorySegment ms, int offset, int length, long seed) {
        assert (length >= 0) : "lengthInBytes cannot be negative";
        long hash = hashBytesByWords(ms, offset, length, seed);
        int end = offset + length;
        offset += length & -8;

        if (offset + 4L <= end) {
            hash ^= (ms.getInt(offset) & 0xFFFFFFFFL) * PRIME64_1;
            hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
            offset += 4L;
        }

        while (offset < end) {
            hash ^= (ms.get(offset) & 0xFFL) * PRIME64_5;
            hash = Long.rotateLeft(hash, 11) * PRIME64_1;
            offset++;
        }
        return fmix(hash);
    }

    private static long fmix(long hash) {
        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;
        return hash;
    }

    private static long hashBytesByWords(MemorySegment ms, int offset, int length, long seed) {
        int end = offset + length;
        long hash;
        if (length >= 32) {
            int limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            do {
                v1 += ms.getLong(offset) * PRIME64_2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= PRIME64_1;

                v2 += ms.getLong(offset + 8) * PRIME64_2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= PRIME64_1;

                v3 += ms.getLong(offset + 16) * PRIME64_2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= PRIME64_1;

                v4 += ms.getLong(offset + 24) * PRIME64_2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= PRIME64_1;

                offset += 32L;
            } while (offset <= limit);

            hash =
                    Long.rotateLeft(v1, 1)
                            + Long.rotateLeft(v2, 7)
                            + Long.rotateLeft(v3, 12)
                            + Long.rotateLeft(v4, 18);

            v1 *= PRIME64_2;
            v1 = Long.rotateLeft(v1, 31);
            v1 *= PRIME64_1;
            hash ^= v1;
            hash = hash * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2;
            v2 = Long.rotateLeft(v2, 31);
            v2 *= PRIME64_1;
            hash ^= v2;
            hash = hash * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2;
            v3 = Long.rotateLeft(v3, 31);
            v3 *= PRIME64_1;
            hash ^= v3;
            hash = hash * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2;
            v4 = Long.rotateLeft(v4, 31);
            v4 *= PRIME64_1;
            hash ^= v4;
            hash = hash * PRIME64_1 + PRIME64_4;
        } else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        int limit = end - 8;
        while (offset <= limit) {
            long k1 = ms.getLong(offset);
            hash ^= Long.rotateLeft(k1 * PRIME64_2, 31) * PRIME64_1;
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
            offset += 8L;
        }
        return hash;
    }
}
