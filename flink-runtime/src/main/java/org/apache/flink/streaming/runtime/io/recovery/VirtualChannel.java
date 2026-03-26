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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;

/**
 * Represents a virtual channel for demultiplexing records during recovery.
 *
 * <p>A virtual channel wraps a {@link RecordDeserializer} and adds record filtering capability,
 * along with tracking watermark and watermark status state.
 *
 * @param <T> The type of record values.
 */
@Internal
public class VirtualChannel<T> {
    private final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer;
    private final RecordFilter<T> recordFilter;

    private Watermark lastWatermark = Watermark.UNINITIALIZED;
    private WatermarkStatus watermarkStatus = WatermarkStatus.ACTIVE;
    private DeserializationResult lastResult;

    public VirtualChannel(
            RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer,
            RecordFilter<T> recordFilter) {
        this.deserializer = deserializer;
        this.recordFilter = recordFilter;
    }

    /**
     * Deserializes the next record from the buffer, applying the record filter.
     *
     * <p>This method loops through records until it finds one that passes the filter or the buffer
     * is consumed. Watermarks and watermark statuses are always accepted and their state is
     * updated.
     *
     * @param delegate The deserialization delegate to populate with the record.
     * @return The deserialization result indicating whether a full record was read.
     * @throws IOException If an I/O error occurs during deserialization.
     */
    public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
            throws IOException {
        do {
            lastResult = deserializer.getNextRecord(delegate);

            if (lastResult.isFullRecord()) {
                final StreamElement element = delegate.getInstance();
                // test if record belongs to this subtask if it comes from ambiguous channel
                if (element.isRecord() && recordFilter.filter(element.asRecord())) {
                    return lastResult;
                } else if (element.isWatermark()) {
                    lastWatermark = element.asWatermark();
                    return lastResult;
                } else if (element.isWatermarkStatus()) {
                    watermarkStatus = element.asWatermarkStatus();
                    return lastResult;
                }
            }
            // loop is only re-executed for filtered full records
        } while (!lastResult.isBufferConsumed());
        return DeserializationResult.PARTIAL_RECORD;
    }

    /**
     * Sets the next buffer to be deserialized.
     *
     * @param buffer The buffer containing serialized records.
     * @throws IOException If an I/O error occurs.
     */
    public void setNextBuffer(Buffer buffer) throws IOException {
        deserializer.setNextBuffer(buffer);
    }

    /** Clears the deserializer state. */
    public void clear() {
        deserializer.clear();
    }

    /**
     * Checks if there is partial data remaining in the buffer.
     *
     * @return true if the last result indicates the buffer was not fully consumed.
     */
    public boolean hasPartialData() {
        return lastResult != null && !lastResult.isBufferConsumed();
    }

    /**
     * Gets the last watermark received on this virtual channel.
     *
     * @return The last watermark, or {@link Watermark#UNINITIALIZED} if none received yet.
     */
    public Watermark getLastWatermark() {
        return lastWatermark;
    }

    /**
     * Gets the current watermark status of this virtual channel.
     *
     * @return The current watermark status.
     */
    public WatermarkStatus getWatermarkStatus() {
        return watermarkStatus;
    }
}
