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

package org.apache.flink.table.runtime.operators.multipleinput.output;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputTestBase;
import org.apache.flink.table.runtime.operators.multipleinput.TestingOneInputStreamOperator;
import org.apache.flink.table.runtime.operators.multipleinput.TestingTwoInputStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the sub-classes of {@link Output}. */
public class OutputTest extends MultipleInputTestBase {

    private StreamRecord<RowData> element;
    private Watermark watermark;
    private LatencyMarker latencyMarker;
    private TypeSerializer<RowData> serializer;

    @Before
    public void setup() {
        element = new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
        watermark = new Watermark(1223456789);
        latencyMarker = new LatencyMarker(122345678, new OperatorID(123, 456), 1);
        serializer =
                InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType()))
                        .createSerializer(new ExecutionConfig());
    }

    @Test
    public void testOneInput() throws Exception {
        TestingOneInputStreamOperator op = createOneInputStreamOperator();
        OneInputStreamOperatorOutput output = new OneInputStreamOperatorOutput(op);

        output.collect(element);
        assertThat(op.getCurrentElement()).isEqualTo(element);

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark()).isEqualTo(watermark);

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker()).isEqualTo(latencyMarker);
    }

    @Test
    public void testCopyingOneInput() throws Exception {
        TestingOneInputStreamOperator op = createOneInputStreamOperator();
        CopyingOneInputStreamOperatorOutput output =
                new CopyingOneInputStreamOperatorOutput(op, serializer);

        output.collect(element);
        assertThat(op.getCurrentElement()).isNotSameAs(element);
        assertThat(op.getCurrentElement()).isEqualTo(element);

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark()).isSameAs(watermark);

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker()).isSameAs(latencyMarker);
    }

    @Test
    public void testFirstInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        FirstInputOfTwoInputStreamOperatorOutput output =
                new FirstInputOfTwoInputStreamOperatorOutput(op);

        output.collect(element);
        assertThat(op.getCurrentElement1()).isEqualTo(element);
        assertThat(op.getCurrentElement2()).isNull();

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark1()).isEqualTo(watermark);
        assertThat(op.getCurrentWatermark2()).isNull();

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isEqualTo(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isNull();
    }

    @Test
    public void testCopyingFirstInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        CopyingFirstInputOfTwoInputStreamOperatorOutput output =
                new CopyingFirstInputOfTwoInputStreamOperatorOutput(op, serializer);

        output.collect(element);
        assertThat(op.getCurrentElement1()).isNotSameAs(element);
        assertThat(op.getCurrentElement1()).isEqualTo(element);
        assertThat(op.getCurrentElement2()).isNull();

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark1()).isSameAs(watermark);
        assertThat(op.getCurrentWatermark2()).isNull();

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isSameAs(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isNull();
    }

    @Test
    public void testSecondInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        SecondInputOfTwoInputStreamOperatorOutput output =
                new SecondInputOfTwoInputStreamOperatorOutput(op);

        output.collect(element);
        assertThat(op.getCurrentElement2()).isEqualTo(element);
        assertThat(op.getCurrentElement1()).isNull();

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark2()).isEqualTo(watermark);
        assertThat(op.getCurrentWatermark1()).isNull();

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isEqualTo(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isNull();
    }

    @Test
    public void testCopyingSecondInputOfTwoInput() throws Exception {
        TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
        CopyingSecondInputOfTwoInputStreamOperatorOutput output =
                new CopyingSecondInputOfTwoInputStreamOperatorOutput(op, serializer);

        output.collect(element);
        assertThat(op.getCurrentElement2()).isNotSameAs(element);
        assertThat(op.getCurrentElement2()).isEqualTo(element);
        assertThat(op.getCurrentElement1()).isNull();

        output.emitWatermark(watermark);
        assertThat(op.getCurrentWatermark2()).isSameAs(watermark);
        assertThat(op.getCurrentWatermark1()).isNull();

        output.emitLatencyMarker(latencyMarker);
        assertThat(op.getCurrentLatencyMarker2()).isSameAs(latencyMarker);
        assertThat(op.getCurrentLatencyMarker1()).isNull();
    }

    @Test
    public void testBroadcasting() throws Exception {
        TestingOneInputStreamOperator op1 = createOneInputStreamOperator();
        TestingOneInputStreamOperator op2 = createOneInputStreamOperator();
        BroadcastingOutput output =
                new BroadcastingOutput(
                        new Output[] {
                            new OneInputStreamOperatorOutput(op1),
                            new OneInputStreamOperatorOutput(op2)
                        });

        output.collect(element);
        assertThat(op1.getCurrentElement()).isEqualTo(element);
        assertThat(op2.getCurrentElement()).isEqualTo(element);

        output.emitWatermark(watermark);
        assertThat(op1.getCurrentWatermark()).isEqualTo(watermark);
        assertThat(op2.getCurrentWatermark()).isEqualTo(watermark);

        // random choose one output to emit LatencyMarker
        output.emitLatencyMarker(latencyMarker);
        if (op1.getCurrentLatencyMarker() != null) {
            assertThat(op1.getCurrentLatencyMarker()).isEqualTo(latencyMarker);
            assertThat(op2.getCurrentLatencyMarker()).isNull();
        } else {
            assertThat(op2.getCurrentLatencyMarker()).isEqualTo(latencyMarker);
        }
    }

    @Test
    public void testCopyingBroadcasting() throws Exception {
        TestingOneInputStreamOperator op1 = createOneInputStreamOperator();
        TestingOneInputStreamOperator op2 = createOneInputStreamOperator();
        CopyingBroadcastingOutput output =
                new CopyingBroadcastingOutput(
                        new Output[] {
                            new OneInputStreamOperatorOutput(op1),
                            new OneInputStreamOperatorOutput(op2)
                        });

        output.collect(element);
        assertThat(op1.getCurrentElement()).isNotSameAs(element);
        assertThat(op1.getCurrentElement()).isEqualTo(element);
        // the last element does not need copy
        assertThat(op2.getCurrentElement()).isSameAs(element);

        output.emitWatermark(watermark);
        assertThat(op1.getCurrentWatermark()).isSameAs(watermark);
        assertThat(op2.getCurrentWatermark()).isSameAs(watermark);

        // random choose one output to emit LatencyMarker
        output.emitLatencyMarker(latencyMarker);
        if (op1.getCurrentLatencyMarker() != null) {
            assertThat(op1.getCurrentLatencyMarker()).isSameAs(latencyMarker);
            assertThat(op2.getCurrentLatencyMarker()).isNull();
        } else {
            assertThat(op2.getCurrentLatencyMarker()).isSameAs(latencyMarker);
        }
    }
}
