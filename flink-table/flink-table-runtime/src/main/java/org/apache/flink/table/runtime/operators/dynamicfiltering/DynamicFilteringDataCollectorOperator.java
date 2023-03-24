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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operator to collect and build the {@link DynamicFilteringData} for sources that supports dynamic
 * filtering.
 */
public class DynamicFilteringDataCollectorOperator extends AbstractStreamOperator<Object>
        implements OneInputStreamOperator<RowData, Object> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicFilteringDataCollectorOperator.class);

    private final RowType dynamicFilteringFieldType;
    private final List<Integer> dynamicFilteringFieldIndices;
    private final long threshold;
    private final OperatorEventGateway operatorEventGateway;

    private transient TypeInformation<RowData> typeInfo;
    private transient TypeSerializer<RowData> serializer;

    private transient Set<byte[]> buffer;
    private transient long currentSize;
    private transient FieldGetter[] fieldGetters;

    public DynamicFilteringDataCollectorOperator(
            RowType dynamicFilteringFieldType,
            List<Integer> dynamicFilteringFieldIndices,
            long threshold,
            OperatorEventGateway operatorEventGateway) {
        this.dynamicFilteringFieldType = checkNotNull(dynamicFilteringFieldType);
        this.dynamicFilteringFieldIndices = checkNotNull(dynamicFilteringFieldIndices);
        this.threshold = threshold;
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.typeInfo = InternalTypeInfo.of(dynamicFilteringFieldType);
        this.serializer = typeInfo.createSerializer(new ExecutionConfig());
        this.buffer = new TreeSet<>(new BytePrimitiveArrayComparator(true)::compare);
        this.currentSize = 0L;
        this.fieldGetters =
                IntStream.range(0, dynamicFilteringFieldIndices.size())
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                dynamicFilteringFieldType.getTypeAt(i),
                                                dynamicFilteringFieldIndices.get(i)))
                        .toArray(FieldGetter[]::new);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (exceedThreshold()) {
            return;
        }

        RowData value = element.getValue();
        GenericRowData rowData = new GenericRowData(dynamicFilteringFieldIndices.size());
        for (int i = 0; i < dynamicFilteringFieldIndices.size(); ++i) {
            rowData.setField(i, fieldGetters[i].getFieldOrNull(value));
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            serializer.serialize(rowData, wrapper);
            boolean duplicated = !buffer.add(baos.toByteArray());
            if (duplicated) {
                return;
            }

            currentSize += baos.size();
        }

        if (exceedThreshold()) {
            // Clear the filtering data and disable self by leaving the currentSize unchanged
            buffer.clear();
            LOG.warn(
                    "Collected data size exceeds the threshold, {} > {}, dynamic filtering is disabled.",
                    currentSize,
                    threshold);
        }
    }

    private boolean exceedThreshold() {
        return threshold > 0 && currentSize > threshold;
    }

    @Override
    public void finish() throws Exception {
        if (exceedThreshold()) {
            LOG.info(
                    "Finish collecting. {} bytes are collected which exceeds the threshold {}. Sending empty data.",
                    currentSize,
                    threshold);
        } else {
            LOG.info(
                    "Finish collecting. {} bytes in {} rows are collected. Sending the data.",
                    currentSize,
                    buffer.size());
        }
        sendEvent();
    }

    private void sendEvent() {
        final DynamicFilteringData dynamicFilteringData;
        if (exceedThreshold()) {
            dynamicFilteringData =
                    new DynamicFilteringData(
                            typeInfo, dynamicFilteringFieldType, Collections.emptyList(), false);
        } else {
            dynamicFilteringData =
                    new DynamicFilteringData(
                            typeInfo, dynamicFilteringFieldType, new ArrayList<>(buffer), true);
        }

        DynamicFilteringEvent event = new DynamicFilteringEvent(dynamicFilteringData);
        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (buffer != null) {
            buffer.clear();
        }
    }
}
