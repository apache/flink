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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * The variant of {@link PullingAsyncDataInput} that is defined for handling both network input and
 * source input in a unified way via {@link #emitNext(DataOutput)} instead of returning {@code
 * Optional.empty()} via {@link PullingAsyncDataInput#pollNext()}.
 */
@Internal
public interface PushingAsyncDataInput<T> extends AvailabilityProvider {

    /**
     * Pushes the next element to the output from current data input, and returns the input status
     * to indicate whether there are more available data in current input.
     *
     * <p>This method should be non blocking.
     */
    DataInputStatus emitNext(DataOutput<T> output) throws Exception;

    /**
     * Basic data output interface used in emitting the next element from data input.
     *
     * @param <T> The type encapsulated with the stream record.
     */
    interface DataOutput<T> {

        void emitRecord(StreamRecord<T> streamRecord) throws Exception;

        void emitWatermark(Watermark watermark) throws Exception;

        void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception;

        void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception;
    }
}
