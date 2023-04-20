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

package org.apache.flink.table.runtime.operators.source;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import javax.annotation.Nullable;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InputConversionOperator}. */
public class InputConversionOperatorTest {

    @Test
    public void testInvalidRecords() {
        final InputConversionOperator<Row> operator =
                new InputConversionOperator<>(
                        createConverter(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT()))),
                        false,
                        false,
                        false,
                        true);

        // invalid record due to missing field
        assertThatThrownBy(
                        () ->
                                operator.processElement(
                                        new StreamRecord<>(Row.ofKind(RowKind.INSERT))))
                .satisfies(
                        anyCauseMatches(
                                FlinkRuntimeException.class,
                                "Error during input conversion from external DataStream "
                                        + "API to internal Table API data structures"));

        // invalid row kind
        assertThatThrownBy(
                        () ->
                                operator.processElement(
                                        new StreamRecord<>(Row.ofKind(RowKind.DELETE, 12))))
                .satisfies(
                        anyCauseMatches(
                                FlinkRuntimeException.class,
                                "Conversion expects insert-only records"));
    }

    @Test
    public void testInvalidEventTime() {
        final InputConversionOperator<Row> operator =
                new InputConversionOperator<>(
                        createConverter(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT()))),
                        false,
                        true,
                        false,
                        true);
        assertThatThrownBy(
                        () ->
                                operator.processElement(
                                        new StreamRecord<>(Row.ofKind(RowKind.INSERT, 12))))
                .satisfies(
                        anyCauseMatches(
                                FlinkRuntimeException.class,
                                "Could not find timestamp in DataStream API record."));
    }

    @Test
    public void testWatermarkSuppression() throws Exception {
        final InputConversionOperator<Row> operator =
                new InputConversionOperator<>(
                        createConverter(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT()))),
                        false,
                        false,
                        false,
                        true);

        // would throw an exception otherwise because an output is not set
        operator.processWatermark(new Watermark(1000));
    }

    @Test(expected = NullPointerException.class)
    public void testReceiveMaxWatermark() throws Exception {
        final InputConversionOperator<Row> operator =
                new InputConversionOperator<>(
                        createConverter(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT()))),
                        false,
                        false,
                        false,
                        true);

        // would throw an exception because it always emits Watermark.MAX_WATERMARK
        operator.processWatermark(Watermark.MAX_WATERMARK);
    }

    private static DynamicTableSource.DataStructureConverter createConverter(DataType dataType) {
        final DataStructureConverter<Object, Object> converter =
                DataStructureConverters.getConverter(dataType);
        return new DynamicTableSource.DataStructureConverter() {

            @Override
            public @Nullable Object toInternal(@Nullable Object externalStructure) {
                return converter.toInternalOrNull(externalStructure);
            }

            @Override
            public void open(Context context) {
                // nothing to do
            }
        };
    }
}
