/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.heap;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.writable.WritableTimestampVector;

import java.util.Arrays;

/** This class represents a nullable byte column vector. */
public class HeapTimestampVector extends AbstractHeapVector implements WritableTimestampVector {

    private static final long serialVersionUID = 1L;

    private final long[] milliseconds;
    private final int[] nanoOfMilliseconds;

    public HeapTimestampVector(int len) {
        super(len);
        this.milliseconds = new long[len];
        this.nanoOfMilliseconds = new int[len];
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        if (dictionary == null) {
            return TimestampData.fromEpochMillis(milliseconds[i], nanoOfMilliseconds[i]);
        } else {
            return dictionary.decodeToTimestamp(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setTimestamp(int i, TimestampData timestamp) {
        milliseconds[i] = timestamp.getMillisecond();
        nanoOfMilliseconds[i] = timestamp.getNanoOfMillisecond();
    }

    @Override
    public void fill(TimestampData value) {
        Arrays.fill(milliseconds, value.getMillisecond());
        Arrays.fill(nanoOfMilliseconds, value.getNanoOfMillisecond());
    }
}
