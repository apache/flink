/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Base class for broadcast stream operator test harnesses. */
public abstract class AbstractBroadcastStreamOperatorTestHarness<IN1, IN2, OUT>
        extends AbstractStreamOperatorTestHarness<OUT> {

    public AbstractBroadcastStreamOperatorTestHarness(
            StreamOperator<OUT> operator, int maxParallelism, int parallelism, int subtaskIndex)
            throws Exception {
        super(operator, maxParallelism, parallelism, subtaskIndex);
    }

    public TwoInputStreamOperator<IN1, IN2, OUT> getTwoInputOperator() {
        return (TwoInputStreamOperator<IN1, IN2, OUT>) operator;
    }

    public void processElement(StreamRecord<IN1> element) throws Exception {
        getTwoInputOperator().setKeyContextElement1(element);
        getTwoInputOperator().processElement1(element);
    }

    public void processElement(IN1 value, long timestamp) throws Exception {
        processElement(new StreamRecord<>(value, timestamp));
    }

    public void processBroadcastElement(StreamRecord<IN2> element) throws Exception {
        getTwoInputOperator().setKeyContextElement2(element);
        getTwoInputOperator().processElement2(element);
    }

    public void processBroadcastElement(IN2 value, long timestamp) throws Exception {
        StreamRecord<IN2> element = new StreamRecord<>(value, timestamp);
        processBroadcastElement(element);
    }

    public void processWatermark(Watermark mark) throws Exception {
        getOperator().processWatermark1(mark);
    }

    public void processBroadcastWatermark(Watermark mark) throws Exception {
        getOperator().processWatermark2(mark);
    }

    public void processWatermark(long timestamp) throws Exception {
        Watermark mark = new Watermark(timestamp);
        getOperator().processWatermark1(mark);
    }

    public void processBroadcastWatermark(long timestamp) throws Exception {
        Watermark mark = new Watermark(timestamp);
        getOperator().processWatermark2(mark);
    }

    public void watermark(long timestamp) throws Exception {
        processWatermark(new Watermark(timestamp));
        processBroadcastWatermark(new Watermark(timestamp));
    }
}
