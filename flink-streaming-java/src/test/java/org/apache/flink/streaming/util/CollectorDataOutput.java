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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.List;

/** Collecting {@link DataOutput} for {@link StreamRecord}. */
public class CollectorDataOutput<T> implements DataOutput<T> {

    private final List<StreamElement> streamElements;

    public CollectorDataOutput(List<StreamElement> streamElements) {
        this.streamElements = streamElements;
    }

    @Override
    public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
        try {
            ClassLoader classLoader = streamRecord.getClass().getClassLoader();
            T copied =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(streamRecord.getValue()),
                            classLoader);
            streamElements.add(streamRecord.copy(copied));
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException("Unable to deserialize record: " + streamRecord, ex);
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        streamElements.add(watermark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        streamElements.add(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        streamElements.add(latencyMarker);
    }
}
