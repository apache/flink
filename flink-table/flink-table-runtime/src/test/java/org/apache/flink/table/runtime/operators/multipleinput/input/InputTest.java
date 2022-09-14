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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputTestBase;
import org.apache.flink.table.runtime.operators.multipleinput.TestingOneInputStreamOperator;
import org.apache.flink.table.runtime.operators.multipleinput.TestingTwoInputStreamOperator;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the sub-classes of {@link Input}. */
public class InputTest extends MultipleInputTestBase {

    private StreamRecord<RowData> element;
    private Watermark watermark;
    private LatencyMarker latencyMarker;

    @Before
    public void setup() {
        element = new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
        watermark = new Watermark(1223456789);
        latencyMarker = new LatencyMarker(122345678, new OperatorID(123, 456), 1);
    }

    @Test
    public void testOneInput() throws Exception {
        TestingOneInputStreamOperator op = createOneInputStreamOperator();
        OneInput input = new OneInput(op);

        input.processElement(element);
        assertThat(op.getCurrentElement()).isEqualTo(element);

        input.processWatermark(watermark);
        assertThat(op.getCurrentWatermark()).isEqualTo(watermark);

        input.processLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker()).isEqualTo(latencyMarker);
    }

    @Test
    public void testFirstInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        FirstInputOfTwoInput input = new FirstInputOfTwoInput(op);

        input.processElement(element);
        assertThat(op.getCurrentElement1()).isEqualTo(element);
        assertThat(op.getCurrentElement2()).isNull();

        input.processWatermark(watermark);
        assertThat(op.getCurrentWatermark1()).isEqualTo(watermark);
        assertThat(op.getCurrentWatermark2()).isNull();

        input.processLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isEqualTo(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isNull();
    }

    @Test
    public void testSecondInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        SecondInputOfTwoInput input = new SecondInputOfTwoInput(op);

        input.processElement(element);
        assertThat(op.getCurrentElement2()).isEqualTo(element);
        assertThat(op.getCurrentElement1()).isNull();

        input.processWatermark(watermark);
        assertThat(op.getCurrentWatermark2()).isEqualTo(watermark);
        assertThat(op.getCurrentWatermark1()).isNull();

        input.processLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isEqualTo(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isNull();
    }
}
