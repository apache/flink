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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.List;

/** Collecting {@link Output} for {@link StreamRecord}. */
public class CollectorOutput<T> implements Output<StreamRecord<T>> {

    private final List<StreamElement> list;

    public CollectorOutput(List<StreamElement> list) {
        this.list = list;
    }

    @Override
    public void emitWatermark(Watermark mark) {
        list.add(mark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        list.add(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        list.add(latencyMarker);
    }

    @Override
    public void collect(StreamRecord<T> record) {
        try {
            ClassLoader cl = record.getClass().getClassLoader();
            T copied =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(record.getValue()), cl);
            list.add(record.copy(copied));
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException("Unable to deserialize record: " + record, ex);
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        throw new UnsupportedOperationException("Side output not supported for CollectorOutput");
    }

    @Override
    public void close() {}
}
