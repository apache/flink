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

package org.apache.flink.connector.pulsar.table.source.impl;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

// TODO these classes needs to be serializable
public class PulsarProjectProducedRowSupport implements Serializable {
    private static final long serialVersionUID = -3399264407634977459L;

    private final int physicalArity;

    private final int[] keyProjection;

    private final int[] valueProjection;

    private final PulsarAppendMetadataSupport appendMetadataSupport;

    private final PulsarUpsertSupport upsertSupport;

    public PulsarProjectProducedRowSupport(
            int physicalArity,
            int[] keyProjection,
            int[] valueProjection,
            PulsarAppendMetadataSupport appendMetadataSupport,
            PulsarUpsertSupport upsertSupport) {
        this.physicalArity = physicalArity;
        this.keyProjection = keyProjection;
        this.valueProjection = valueProjection;
        this.appendMetadataSupport = appendMetadataSupport;
        this.upsertSupport = upsertSupport;
    }

    public void projectToProducedRowAndCollect(
            Message<?> message,
            List<RowData> keyRowDataList,
            List<RowData> valueRowDataList,
            Collector<RowData> collector) {
        // no key defined
        // TODO further refactor needed, assert on keyRowDataList null/empty
        // TODO does the order here matter?
        if (hasNoKeyProjection()) {
            valueRowDataList.forEach(
                    valueRow -> emitRow(null, (GenericRowData) valueRow, collector, message));
        } else {
            // TODO what does it mean to emit a value for each key ?
            // otherwise emit a value for each key
            valueRowDataList.forEach(
                    valueRow ->
                            keyRowDataList.forEach(
                                    keyRow ->
                                            emitRow(
                                                    (GenericRowData) keyRow,
                                                    (GenericRowData) valueRow,
                                                    collector,
                                                    message)));
        }
    }

    private void emitRow(
            @Nullable GenericRowData physicalKeyRow,
            @Nullable GenericRowData physicalValueRow,
            Collector<RowData> collector,
            Message<?> message) {

        final RowKind rowKind = upsertSupport.decideRowKind(physicalValueRow);
        // TODO need to combine physicalArity and metadataArity
        // TODO metadata arity
        final GenericRowData producedRow =
                new GenericRowData(
                        rowKind, physicalArity + appendMetadataSupport.getMetadataArity());

        // TODO project values
        if (physicalValueRow != null) {
            for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                producedRow.setField(
                        valueProjection[valuePos], physicalValueRow.getField(valuePos));
            }
        }
        // TODO put both key and value fields in the produced row
        for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
            assert physicalKeyRow != null;
            producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
        }

        appendMetadataSupport.appendProducedRowWithMetadata(producedRow, physicalArity, message);
        collector.collect(producedRow);
    }

    private boolean hasNoKeyProjection() {
        return keyProjection.length == 0;
    }
}
