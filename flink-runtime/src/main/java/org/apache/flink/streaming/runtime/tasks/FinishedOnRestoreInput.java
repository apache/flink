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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/** A fake {@link Input} for finished on restore tasks. */
public class FinishedOnRestoreInput<IN> implements Input<IN> {
    private final RecordWriterOutput<?>[] streamOutputs;
    private final int inputCount;
    private final String taskName;

    private int watermarksSeen = 0;
    private int finishedStatusSeen = 0;

    public FinishedOnRestoreInput(
            RecordWriterOutput<?>[] streamOutputs, int inputCount, String taskName) {
        this.streamOutputs = streamOutputs;
        this.inputCount = inputCount;
        this.taskName = taskName;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        throw new IllegalStateException();
    }

    @Override
    public void processWatermark(Watermark watermark) {
        if (watermark.getTimestamp() != Watermark.MAX_WATERMARK.getTimestamp()) {
            throw new IllegalStateException(
                    String.format(
                            "We should not receive any watermarks [%s] other than the MAX_WATERMARK if finished on restore. Task: %s",
                            watermark, taskName));
        }
        if (++watermarksSeen == inputCount) {
            for (RecordWriterOutput<?> streamOutput : streamOutputs) {
                streamOutput.emitWatermark(watermark);
            }
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        // A task that finished on restore only ever sees FINISHED from upstream; ACTIVE and IDLE
        // describe a temporary state that cannot apply to an input which is already complete.
        if (!watermarkStatus.isFinished()) {
            throw new IllegalStateException(
                    String.format(
                            "Unexpected watermark status [%s] received for finished on restore task. Task: %s",
                            watermarkStatus, taskName));
        }
        // Aggregate FINISHED across all inputs and forward it once every input has reported,
        // mirroring how MAX_WATERMARK is handled above.
        if (++finishedStatusSeen == inputCount) {
            for (RecordWriterOutput<?> streamOutput : streamOutputs) {
                streamOutput.emitWatermarkStatus(watermarkStatus);
            }
        }
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        throw new IllegalStateException();
    }

    @Override
    public void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        throw new IllegalStateException();
    }
}
