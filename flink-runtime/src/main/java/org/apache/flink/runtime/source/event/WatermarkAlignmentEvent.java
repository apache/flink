/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/** Signals source operators the maximum watermark that emitted records can have. */
public class WatermarkAlignmentEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long maxWatermark;

    public WatermarkAlignmentEvent(long maxWatermark) {
        this.maxWatermark = maxWatermark;
    }

    public long getMaxWatermark() {
        return maxWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkAlignmentEvent that = (WatermarkAlignmentEvent) o;
        return maxWatermark == that.maxWatermark;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxWatermark);
    }

    @Override
    public String toString() {
        return "WatermarkAlignmentEvent{" + "maxWatermark=" + maxWatermark + '}';
    }
}
