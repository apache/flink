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

package org.apache.flink.types;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.MemorySegment;

/**
 * The base interface for normalizable keys. Normalizable keys can create a binary representation of
 * themselves that is byte-wise comparable. The byte-wise comparison of two normalized keys proceeds
 * until all bytes are compared or two bytes at the corresponding positions are not equal. If two
 * corresponding byte values are not equal, the lower byte value indicates the lower key. If both
 * normalized keys are byte-wise identical, the actual key may have to be looked at to determine
 * which one is actually lower.
 *
 * <p>The latter depends on whether the normalized key covers the entire key or is just a prefix of
 * the key. A normalized key is considered a prefix, if its length is less than the maximal
 * normalized key length.
 */
@Public
public interface NormalizableKey<T> extends Comparable<T>, Key<T> {

    /**
     * Gets the maximal length of normalized keys that the data type would produce to determine the
     * order of instances solely by the normalized key. A value of {@link
     * java.lang.Integer}.MAX_VALUE is interpreted as infinite.
     *
     * <p>For example, 32 bit integers return four, while Strings (potentially unlimited in length)
     * return {@link java.lang.Integer}.MAX_VALUE.
     *
     * @return The maximal length of normalized keys.
     */
    int getMaxNormalizedKeyLen();

    /**
     * Writes a normalized key for the given record into the target byte array, starting at the
     * specified position an writing exactly the given number of bytes. Note that the comparison of
     * the bytes is treating the bytes as unsigned bytes: {@code int byteI = bytes[i] & 0xFF;}
     *
     * <p>If the meaningful part of the normalized key takes less than the given number of bytes,
     * then it must be padded. Padding is typically required for variable length data types, such as
     * strings. The padding uses a special character, either {@code 0} or {@code 0xff}, depending on
     * whether shorter values are sorted to the beginning or the end.
     *
     * @param memory The memory segment to put the normalized key bytes into.
     * @param offset The offset in the byte array where the normalized key's bytes should start.
     * @param len The number of bytes to put.
     */
    void copyNormalizedKey(MemorySegment memory, int offset, int len);
}
