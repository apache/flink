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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;

import java.util.ArrayList;
import java.util.List;

/** This counter will count the data size of a partition. */
public class ResultPartitionBytesCounter {

    /** The data size of each subpartition. */
    private final List<Counter> subpartitionBytes;

    public ResultPartitionBytesCounter(int numSubpartitions) {
        this.subpartitionBytes = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; ++i) {
            subpartitionBytes.add(new SimpleCounter());
        }
    }

    public void inc(int targetSubpartition, long bytes) {
        subpartitionBytes.get(targetSubpartition).inc(bytes);
    }

    public void incAll(long bytes) {
        subpartitionBytes.forEach(counter -> counter.inc(bytes));
    }

    public ResultPartitionBytes createSnapshot() {
        return new ResultPartitionBytes(
                subpartitionBytes.stream().mapToLong(Counter::getCount).toArray());
    }
}
