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

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * A test harness for testing a {@link MultipleInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements and
 * watermarks can be retrieved. you are free to modify these.
 */
public class MultiInputStreamOperatorTestHarness<OUT>
        extends AbstractStreamOperatorTestHarness<OUT> {

    public MultiInputStreamOperatorTestHarness(StreamOperatorFactory<OUT> operator)
            throws Exception {
        this(operator, 1, 1, 0);
    }

    public MultiInputStreamOperatorTestHarness(
            StreamOperatorFactory<OUT> operatorFactory,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex)
            throws Exception {
        super(operatorFactory, maxParallelism, numSubtasks, subtaskIndex);
    }

    public void processElement(int idx, StreamRecord<?> element) throws Exception {
        Input input = getCastedOperator().getInputs().get(idx);
        input.setKeyContextElement(element);
        input.processElement(element);
    }

    public void processWatermark(int idx, Watermark mark) throws Exception {
        getCastedOperator().getInputs().get(idx).processWatermark(mark);
    }

    public void processWatermarkStatus(int idx, WatermarkStatus watermarkStatus) throws Exception {
        getCastedOperator().getInputs().get(idx).processWatermarkStatus(watermarkStatus);
    }

    private MultipleInputStreamOperator<OUT> getCastedOperator() {
        return (MultipleInputStreamOperator<OUT>) operator;
    }
}
