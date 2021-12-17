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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

/** Operator that converts to external data structures and unwraps atomic records if necessary. */
@Internal
public class OutputConversionOperator extends TableStreamOperator<Object>
        implements OneInputStreamOperator<RowData, Object> {

    private final @Nullable RowData.FieldGetter atomicFieldGetter;

    private final DataStructureConverter converter;

    private final int rowtimeIndex;

    private final boolean consumeRowtimeMetadata;

    private transient StreamRecord<Object> outRecord;

    public OutputConversionOperator(
            @Nullable RowData.FieldGetter atomicFieldGetter,
            DataStructureConverter converter,
            int rowtimeIndex,
            boolean consumeRowtimeMetadata) {
        this.atomicFieldGetter = atomicFieldGetter;
        this.converter = converter;
        this.rowtimeIndex = rowtimeIndex;
        this.consumeRowtimeMetadata = consumeRowtimeMetadata;
    }

    @Override
    public void open() throws Exception {
        super.open();

        outRecord = new StreamRecord<>(null);

        final Context context = Context.create(getUserCodeClassloader());
        this.converter.open(context);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData rowData = element.getValue();

        if (consumeRowtimeMetadata) {
            // timestamp is TIMESTAMP_LTZ
            final long rowtime = rowData.getTimestamp(rowData.getArity() - 1, 3).getMillisecond();
            outRecord.setTimestamp(rowtime);
        } else if (rowtimeIndex != -1) {
            // timestamp might be TIMESTAMP or TIMESTAMP_LTZ
            final long rowtime = rowData.getTimestamp(rowtimeIndex, 3).getMillisecond();
            outRecord.setTimestamp(rowtime);
        }

        final Object internalRecord;
        if (atomicFieldGetter != null) {
            internalRecord = atomicFieldGetter.getFieldOrNull(rowData);
        } else {
            internalRecord = rowData;
        }

        final Object externalRecord;
        try {
            externalRecord = converter.toExternal(internalRecord);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Error during output conversion from internal Table API to "
                                    + "external DataStream API data structures. Make sure "
                                    + "that the provided data types that configure the "
                                    + "converters are correctly declared in the schema. "
                                    + "Affected record:\n%s",
                            internalRecord),
                    e);
        }
        outRecord.replace(externalRecord);

        output.collect(outRecord);
    }
}
