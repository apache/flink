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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An empty {@link PushingAsyncDataInput.DataOutput} which is used by {@link
 * StreamOneInputProcessor} once an {@link DataInputStatus#END_OF_DATA} is received.
 */
public class FinishedDataOutput<IN> implements PushingAsyncDataInput.DataOutput<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(FinishedDataOutput.class);

    @Override
    public void emitRecord(StreamRecord<IN> streamRecord) throws Exception {
        LOG.debug("Unexpected record after finish() received.");
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        LOG.debug("Unexpected watermark after finish() received.");
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        LOG.debug("Unexpected watermark status after finish() received.");
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        LOG.debug("Unexpected latency marker after finish() received.");
    }
}
