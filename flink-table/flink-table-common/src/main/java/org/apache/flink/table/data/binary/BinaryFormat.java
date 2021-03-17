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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;

/** Binary format spanning {@link MemorySegment}s. */
@Internal
public interface BinaryFormat {

    /**
     * It decides whether to put data in FixLenPart or VarLenPart. See more in {@link
     * BinaryRowData}.
     *
     * <p>If len is less than 8, its binary format is: 1-bit mark(1) = 1, 7-bits len, and 7-bytes
     * data. Data is stored in fix-length part.
     *
     * <p>If len is greater or equal to 8, its binary format is: 1-bit mark(1) = 0, 31-bits offset
     * to the data, and 4-bytes length of data. Data is stored in variable-length part.
     */
    int MAX_FIX_PART_DATA_SIZE = 7;
    /**
     * To get the mark in highest bit of long. Form: 10000000 00000000 ... (8 bytes)
     *
     * <p>This is used to decide whether the data is stored in fixed-length part or variable-length
     * part. see {@link #MAX_FIX_PART_DATA_SIZE} for more information.
     */
    long HIGHEST_FIRST_BIT = 0x80L << 56;
    /**
     * To get the 7 bits length in second bit to eighth bit out of a long. Form: 01111111 00000000
     * ... (8 bytes)
     *
     * <p>This is used to get the length of the data which is stored in this long. see {@link
     * #MAX_FIX_PART_DATA_SIZE} for more information.
     */
    long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

    /** Gets the underlying {@link MemorySegment}s this binary format spans. */
    MemorySegment[] getSegments();

    /** Gets the start offset of this binary data in the {@link MemorySegment}s. */
    int getOffset();

    /** Gets the size in bytes of this binary data. */
    int getSizeInBytes();
}
