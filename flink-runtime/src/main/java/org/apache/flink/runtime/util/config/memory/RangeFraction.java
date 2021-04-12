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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.MemorySize;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Range and fraction of a memory component, which is a capped fraction of another component. */
public class RangeFraction {
    private final MemorySize minSize;
    private final MemorySize maxSize;
    private final double fraction;

    RangeFraction(MemorySize minSize, MemorySize maxSize, double fraction) {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.fraction = fraction;
        checkArgument(
                minSize.getBytes() <= maxSize.getBytes(),
                "min value must be less or equal to max value");
        checkArgument(fraction >= 0 && fraction < 1, "fraction must be in range [0, 1)");
    }

    public MemorySize getMinSize() {
        return minSize;
    }

    public MemorySize getMaxSize() {
        return maxSize;
    }

    public double getFraction() {
        return fraction;
    }
}
