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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.metrics.Counter;

/** All metrics that {@link HsResultPartition} needs to count, except numBytesProduced. */
public class HsOutputMetrics {
    private final Counter numBytesOut;
    private final Counter numBuffersOut;

    public HsOutputMetrics(Counter numBytesOut, Counter numBuffersOut) {
        this.numBytesOut = numBytesOut;
        this.numBuffersOut = numBuffersOut;
    }

    public Counter getNumBytesOut() {
        return numBytesOut;
    }

    public Counter getNumBuffersOut() {
        return numBuffersOut;
    }
}
