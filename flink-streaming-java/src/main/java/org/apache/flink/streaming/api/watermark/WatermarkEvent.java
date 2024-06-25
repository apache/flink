/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * A {@link StreamElement} that ships {@link Watermark}s between operators.
 */
@PublicEvolving
public final class WatermarkEvent extends StreamElement {

    private final Watermark genericWatermark;

    public WatermarkEvent(Watermark genericWatermark) {
        this.genericWatermark = genericWatermark;
    }

    public Watermark getWatermark() {
        return genericWatermark;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        return this == o
                || o != null
                        && o.getClass() == this.getClass()
                        && ((WatermarkEvent) o).genericWatermark.equals(this.getWatermark());
    }

    //    @Override
    //    public int hashCode() {
    //        return (int) (timestamp ^ (timestamp >>> 32));
    //    }
    //
    //    @Override
    //    public String toString() {
    //        return "Watermark @ " + timestamp;
    //    }
}
