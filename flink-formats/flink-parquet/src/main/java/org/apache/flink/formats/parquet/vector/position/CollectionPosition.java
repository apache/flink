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

package org.apache.flink.formats.parquet.vector.position;

import javax.annotation.Nullable;

/** To represent collection's position in repeated type. */
public class CollectionPosition {
    @Nullable private final boolean[] isNull;
    private final long[] offsets;

    private final long[] length;

    private final int valueCount;

    public CollectionPosition(boolean[] isNull, long[] offsets, long[] length, int valueCount) {
        this.isNull = isNull;
        this.offsets = offsets;
        this.length = length;
        this.valueCount = valueCount;
    }

    public boolean[] getIsNull() {
        return isNull;
    }

    public long[] getOffsets() {
        return offsets;
    }

    public long[] getLength() {
        return length;
    }

    public int getValueCount() {
        return valueCount;
    }
}
