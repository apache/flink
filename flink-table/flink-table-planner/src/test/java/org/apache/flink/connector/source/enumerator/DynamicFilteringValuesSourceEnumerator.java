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

package org.apache.flink.connector.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.source.TerminatingLogic;
import org.apache.flink.connector.source.split.ValuesSourcePartitionSplit;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A SplitEnumerator implementation for dynamic filtering source. */
public class DynamicFilteringValuesSourceEnumerator
        implements SplitEnumerator<ValuesSourcePartitionSplit, NoOpEnumState> {
    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicFilteringValuesSourceEnumerator.class);

    private final SplitEnumeratorContext<ValuesSourcePartitionSplit> context;
    private final List<ValuesSourcePartitionSplit> allSplits;
    private final List<String> dynamicFilteringFields;
    private final TerminatingLogic terminatingLogic;
    private transient boolean receivedDynamicFilteringEvent;
    private transient List<ValuesSourcePartitionSplit> remainingSplits;

    public DynamicFilteringValuesSourceEnumerator(
            SplitEnumeratorContext<ValuesSourcePartitionSplit> context,
            TerminatingLogic terminatingLogic,
            List<ValuesSourcePartitionSplit> allSplits,
            List<String> dynamicFilteringFields) {
        this.context = context;
        this.allSplits = allSplits;
        this.terminatingLogic = terminatingLogic;
        this.dynamicFilteringFields = dynamicFilteringFields;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!receivedDynamicFilteringEvent) {
            throw new IllegalStateException("DynamicFilteringEvent has not receive");
        }
        if (remainingSplits.isEmpty()) {
            if (terminatingLogic == TerminatingLogic.INFINITE) {
                context.assignSplit(
                        new ValuesSourcePartitionSplit(
                                Collections.emptyMap(), TerminatingLogic.INFINITE),
                        subtaskId);
            } else {
                context.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}", subtaskId);
            }
        } else {
            ValuesSourcePartitionSplit split = remainingSplits.remove(0);
            LOG.debug("Assigned split to subtask {} : {}", subtaskId, split);
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof DynamicFilteringEvent) {
            LOG.warn("Received DynamicFilteringEvent: {}", subtaskId);
            receivedDynamicFilteringEvent = true;
            DynamicFilteringData dynamicFilteringData =
                    ((DynamicFilteringEvent) sourceEvent).getData();
            assignPartitions(dynamicFilteringData);
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    private void assignPartitions(DynamicFilteringData data) {
        if (data.isFiltering()) {
            remainingSplits = new ArrayList<>();
            for (ValuesSourcePartitionSplit split : allSplits) {
                List<String> values =
                        dynamicFilteringFields.stream()
                                .map(k -> split.getPartition().get(k))
                                .collect(Collectors.toList());
                LOG.info("values: " + values);
                if (data.contains(generateRowData(values, data.getRowType()))) {
                    remainingSplits.add(split);
                }
            }
        } else {
            remainingSplits = new ArrayList<>(allSplits);
        }
        LOG.info("remainingSplits: " + remainingSplits);
    }

    private GenericRowData generateRowData(List<String> partitionValues, RowType rowType) {
        Preconditions.checkArgument(partitionValues.size() == rowType.getFieldCount());
        Object[] values = new Object[partitionValues.size()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case VARCHAR:
                    values[i] = partitionValues.get(i);
                    break;
                case INTEGER:
                    values[i] = Integer.valueOf(partitionValues.get(i));
                    break;
                case BIGINT:
                    values[i] = Long.valueOf(partitionValues.get(i));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            rowType.getTypeAt(i).getTypeRoot() + " is not supported.");
            }
        }
        return GenericRowData.of(values);
    }

    @Override
    public void addSplitsBack(List<ValuesSourcePartitionSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public NoOpEnumState snapshotState(long checkpointId) throws Exception {
        return new NoOpEnumState();
    }

    @Override
    public void close() throws IOException {}
}
