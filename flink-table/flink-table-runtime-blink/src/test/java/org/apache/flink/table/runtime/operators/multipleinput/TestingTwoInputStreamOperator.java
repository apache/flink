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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** A {@link TwoInputStreamOperator} for testing. */
public class TestingTwoInputStreamOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

    private final boolean emitDataInEndInput;
    private final List<StreamRecord<RowData>> receivedElements = new ArrayList<>();

    private boolean isOpened = false;
    private StreamRecord<RowData> currentElement1 = null;
    private StreamRecord<RowData> currentElement2 = null;
    private Watermark currentWatermark1 = null;
    private Watermark currentWatermark2 = null;
    private LatencyMarker currentLatencyMarker1 = null;
    private LatencyMarker currentLatencyMarker2 = null;
    private final List<Integer> endInputs = new ArrayList<>();
    private boolean isDisposed = false;
    private boolean isClosed = false;

    public TestingTwoInputStreamOperator() {
        this(false);
    }

    public TestingTwoInputStreamOperator(boolean emitDataInEndInput) {
        this.emitDataInEndInput = emitDataInEndInput;
    }

    @Override
    public void open() throws Exception {
        isOpened = true;
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        currentElement1 = element;
        if (emitDataInEndInput) {
            receivedElements.add(element);
        } else {
            output.collect(element);
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        currentElement2 = element;
        if (emitDataInEndInput) {
            receivedElements.add(element);
        } else {
            output.collect(element);
        }
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        currentWatermark1 = mark;
        output.emitWatermark(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        currentWatermark2 = mark;
        output.emitWatermark(mark);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        currentLatencyMarker1 = latencyMarker;
        output.emitLatencyMarker(latencyMarker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        currentLatencyMarker2 = latencyMarker;
        output.emitLatencyMarker(latencyMarker);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        endInputs.add(inputId);
        if (emitDataInEndInput) {
            if (endInputs.size() == 2) {
                receivedElements.forEach(output::collect);
            }
        } else {
            Preconditions.checkArgument(receivedElements.isEmpty());
        }
    }

    @Override
    public void dispose() throws Exception {
        this.isDisposed = true;
    }

    @Override
    public void close() throws Exception {
        isClosed = true;
    }

    public boolean isOpened() {
        return isOpened;
    }

    public StreamRecord<RowData> getCurrentElement1() {
        return currentElement1;
    }

    public StreamRecord<RowData> getCurrentElement2() {
        return currentElement2;
    }

    public Watermark getCurrentWatermark1() {
        return currentWatermark1;
    }

    public Watermark getCurrentWatermark2() {
        return currentWatermark2;
    }

    public LatencyMarker getCurrentLatencyMarker1() {
        return currentLatencyMarker1;
    }

    public LatencyMarker getCurrentLatencyMarker2() {
        return currentLatencyMarker2;
    }

    public List<Integer> getEndInputs() {
        return endInputs;
    }

    public boolean isDisposed() {
        return isDisposed;
    }

    public boolean isClosed() {
        return isClosed;
    }
}
