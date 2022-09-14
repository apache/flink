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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * Operator that converts to internal data structures and wraps atomic records if necessary.
 *
 * @param <E> external type
 */
@Internal
public final class InputConversionOperator<E> extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<E, RowData> {

    private final DataStructureConverter converter;

    private final boolean requiresWrapping;

    private final boolean produceRowtimeMetadata;

    private final boolean propagateWatermark;

    private final boolean isInsertOnly;

    private transient StreamRecord<RowData> outRecord;

    public InputConversionOperator(
            DataStructureConverter converter,
            boolean requiresWrapping,
            boolean produceRowtimeMetadata,
            boolean propagateWatermark,
            boolean isInsertOnly) {
        this.converter = converter;
        this.requiresWrapping = requiresWrapping;
        this.produceRowtimeMetadata = produceRowtimeMetadata;
        this.propagateWatermark = propagateWatermark;
        this.isInsertOnly = isInsertOnly;
    }

    @Override
    public void open() throws Exception {
        super.open();

        outRecord = new StreamRecord<>(null);

        final Context context = Context.create(getUserCodeClassloader());
        this.converter.open(context);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (propagateWatermark || Watermark.MAX_WATERMARK.equals(mark)) {
            super.processWatermark(mark);
        }
    }

    @Override
    public void processElement(StreamRecord<E> element) throws Exception {
        final E externalRecord = element.getValue();

        final Object internalRecord;
        try {
            internalRecord = converter.toInternal(externalRecord);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Error during input conversion from external DataStream API to "
                                    + "internal Table API data structures. Make sure that the "
                                    + "provided data types that configure the converters are "
                                    + "correctly declared in the schema. Affected record:\n%s",
                            externalRecord),
                    e);
        }

        final RowData payloadRowData;
        if (requiresWrapping) {
            final GenericRowData wrapped = new GenericRowData(RowKind.INSERT, 1);
            wrapped.setField(0, internalRecord);
            payloadRowData = wrapped;
        } else {
            // top-level records must not be null and will be skipped
            if (internalRecord == null) {
                return;
            }
            payloadRowData = (RowData) internalRecord;
        }

        final RowKind kind = payloadRowData.getRowKind();

        if (isInsertOnly && kind != RowKind.INSERT) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Error during input conversion. Conversion expects insert-only "
                                    + "records but DataStream API record contains: %s",
                            kind));
        }

        if (!produceRowtimeMetadata) {
            output.collect(outRecord.replace(payloadRowData));
            return;
        }

        if (!element.hasTimestamp()) {
            throw new FlinkRuntimeException(
                    "Could not find timestamp in DataStream API record. "
                            + "Make sure that timestamps have been assigned before and "
                            + "the event-time characteristic is enabled.");
        }

        final GenericRowData rowtimeRowData = new GenericRowData(1);
        rowtimeRowData.setField(0, TimestampData.fromEpochMillis(element.getTimestamp()));

        final JoinedRowData joinedRowData = new JoinedRowData(kind, payloadRowData, rowtimeRowData);

        output.collect(outRecord.replace(joinedRowData));
    }
}
