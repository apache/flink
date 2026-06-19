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

package org.apache.flink.runtime.event;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * This event wraps the {@link Watermark}, it is used in propagate {@link Watermark} between shuffle
 * components, and should not be visible to operators and functions.
 */
@Internal
public class WatermarkEvent extends RuntimeEvent {

    private static final int TAG_LONG_GENERALIZED_WATERMARK = 0;
    private static final int TAG_BOOL_GENERALIZED_WATERMARK = 1;

    private Watermark watermark;
    private boolean isAligned = false;

    public WatermarkEvent() {}

    public WatermarkEvent(Watermark watermark, boolean isAligned) {
        this.watermark = watermark;
        this.isAligned = isAligned;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        // write watermark identifier
        out.writeUTF(watermark.getIdentifier());
        // write align flag
        out.writeBoolean(isAligned);
        // write watermark class tag
        // write watermark value
        if (watermark instanceof LongWatermark) {
            out.writeInt(TAG_LONG_GENERALIZED_WATERMARK);
            out.writeLong(((LongWatermark) watermark).getValue());
        } else if (watermark instanceof BoolWatermark) {
            out.writeInt(TAG_BOOL_GENERALIZED_WATERMARK);
            out.writeBoolean(((BoolWatermark) watermark).getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported watermark type: " + watermark.getClass());
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        // read watermark identifier
        String identifier = in.readUTF();
        // read align flag
        this.isAligned = in.readBoolean();

        // read watermark class tag
        int watermarkTypeTag = in.readInt();

        // read watermark value
        if (watermarkTypeTag == TAG_LONG_GENERALIZED_WATERMARK) {
            long value = in.readLong();
            this.watermark = new LongWatermark(value, identifier);
        } else if (watermarkTypeTag == TAG_BOOL_GENERALIZED_WATERMARK) {
            boolean value = in.readBoolean();
            this.watermark = new BoolWatermark(value, identifier);
        } else {
            throw new IllegalArgumentException("Unknown watermark class tag: " + watermarkTypeTag);
        }
    }

    public Watermark getWatermark() {
        return watermark;
    }

    public boolean isAligned() {
        return isAligned;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkEvent that = (WatermarkEvent) o;
        return isAligned == that.isAligned && Objects.equals(watermark, that.watermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(watermark, isAligned);
    }

    @Override
    public String toString() {
        return "WatermarkEvent{"
                + "watermarkElement="
                + watermark
                + ", isAligned="
                + isAligned
                + '}';
    }
}
