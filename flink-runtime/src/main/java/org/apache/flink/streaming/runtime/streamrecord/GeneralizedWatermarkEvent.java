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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.watermark.GeneralizedWatermark;

import java.util.Objects;

/** Wrapper on top of {@link GeneralizedWatermark} to use it as a {@link StreamElement}. */
@Experimental
public final class GeneralizedWatermarkEvent extends StreamElement {

    private GeneralizedWatermark watermark;

    public GeneralizedWatermarkEvent(GeneralizedWatermark watermark) {
        this.watermark = watermark;
    }

    public GeneralizedWatermark getWatermark() {
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
        GeneralizedWatermarkEvent that = (GeneralizedWatermarkEvent) o;
        return Objects.equals(this.watermark, that.watermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(watermark);
    }

    @Override
    public String toString() {
        return "GeneralizedWatermark{" + "watermark=" + watermark + '}';
    }
}
