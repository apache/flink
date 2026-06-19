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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * Reports last emitted {@link Watermark} from a subtask to the {@link
 * org.apache.flink.runtime.source.coordinator.SourceCoordinator}.
 */
public class ReportedWatermarkEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long watermark;

    public ReportedWatermarkEvent(long watermark) {
        this.watermark = watermark;
    }

    public long getWatermark() {
        return watermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReportedWatermarkEvent that = (ReportedWatermarkEvent) o;
        return watermark == that.watermark;
    }

    @Override
    public int hashCode() {
        return Objects.hash(watermark);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "watermark=" + watermark + '}';
    }
}
