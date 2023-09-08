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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A TopN function could handle updating stream.
 *
 * <p>Input stream can contain any change kind: INSERT, DELETE, UPDATE_BEFORE and UPDATE_AFTER.
 */
public class RetractableTopNFunction extends AbstractTopNFunction {

    private static final long serialVersionUID = 1365312180599454480L;

    private static final Logger LOG = LoggerFactory.getLogger(RetractableTopNFunction.class);

    // Message to indicate the state is cleared because of ttl restriction. The message could be
    // used to output to log.
    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. "
                    + "This will result in incorrect result. You can increase the state ttl to avoid this.";

    private final InternalTypeInfo<RowData> sortKeyType;

    // flag to skip records with non-exist error instead to fail, true by default.
    private final boolean lenient = true;

    // a map state stores mapping from sort key to records list
    private transient MapState<RowData, List<RowData>> dataState;

    // a sorted map stores mapping from sort key to records count
    private transient ValueState<SortedMap<RowData, Long>> treeMap;

    // The util to compare two RowData equals to each other.
    private GeneratedRecordEqualiser generatedEqualiser;
    private RecordEqualiser equaliser;

    private final ComparableRecordComparator serializableComparator;

    private final TypeSerializer<RowData> inputRowSer;

    public RetractableTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            ComparableRecordComparator comparableRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            GeneratedRecordEqualiser generatedEqualiser,
            boolean generateUpdateBefore,
            boolean outputRankNumber) {
        super(
                ttlConfig,
                inputRowType,
                comparableRecordComparator.getGeneratedRecordComparator(),
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
        this.sortKeyType = sortKeySelector.getProducedType();
        this.serializableComparator = comparableRecordComparator;
        this.generatedEqualiser = generatedEqualiser;
        this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // compile equaliser
        equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        generatedEqualiser = null;

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor =
                new MapStateDescriptor<>("data-state", sortKeyType, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = getRuntimeContext().getMapState(mapStateDescriptor);

        ValueStateDescriptor<SortedMap<RowData, Long>> valueStateDescriptor =
                new ValueStateDescriptor<>(
                        "sorted-map",
                        new SortedMapTypeInfo<>(
                                sortKeyType, BasicTypeInfo.LONG_TYPE_INFO, serializableComparator));
        if (ttlConfig.isEnabled()) {
            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }
        treeMap = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        initRankEnd(input);
        SortedMap<RowData, Long> sortedMap = treeMap.value();
        if (sortedMap == null) {
            sortedMap = new TreeMap<>(sortKeyComparator);
        }
        RowData sortKey = sortKeySelector.getKey(input);
        boolean isAccumulate = RowDataUtil.isAccumulateMsg(input);
        input.setRowKind(RowKind.INSERT); // erase row kind for further state accessing
        if (isAccumulate) {
            // update sortedMap
            if (sortedMap.containsKey(sortKey)) {
                sortedMap.put(sortKey, sortedMap.get(sortKey) + 1);
            } else {
                sortedMap.put(sortKey, 1L);
            }

            // emit
            if (outputRankNumber || hasOffset()) {
                // the without-number-algorithm can't handle topN with offset,
                // so use the with-number-algorithm to handle offset
                emitRecordsWithRowNumber(sortedMap, sortKey, input, out);
            } else {
                emitRecordsWithoutRowNumber(sortedMap, sortKey, input, out);
            }
            // update data state
            List<RowData> inputs = dataState.get(sortKey);
            if (inputs == null) {
                // the sort key is never seen
                inputs = new ArrayList<>();
            }
            inputs.add(input);
            dataState.put(sortKey, inputs);
        } else {
            final boolean stateRemoved;
            // emit updates first
            if (outputRankNumber || hasOffset()) {
                // the without-number-algorithm can't handle topN with offset,
                // so use the with-number-algorithm to handle offset
                stateRemoved = retractRecordWithRowNumber(sortedMap, sortKey, input, out);
            } else {
                stateRemoved = retractRecordWithoutRowNumber(sortedMap, sortKey, input, out);
            }

            // and then update sortedMap
            if (sortedMap.containsKey(sortKey)) {
                long count = sortedMap.get(sortKey) - 1;
                if (count == 0) {
                    sortedMap.remove(sortKey);
                } else {
                    sortedMap.put(sortKey, count);
                }
            } else {
                stateStaledErrorHandle();
            }

            if (!stateRemoved) {
                // the input record has not been removed from state
                // should update the data state
                List<RowData> inputs = dataState.get(sortKey);
                if (inputs != null) {
                    // comparing record by equaliser
                    Iterator<RowData> inputsIter = inputs.iterator();
                    while (inputsIter.hasNext()) {
                        if (equaliser.equals(inputsIter.next(), input)) {
                            inputsIter.remove();
                            break;
                        }
                    }
                    if (inputs.isEmpty()) {
                        dataState.remove(sortKey);
                    } else {
                        dataState.put(sortKey, inputs);
                    }
                }
            }
        }
        treeMap.update(sortedMap);
    }

    // ------------- ROW_NUMBER-------------------------------

    private void processStateStaled(Iterator<Map.Entry<RowData, Long>> sortedMapIterator)
            throws RuntimeException {
        // Sync with dataState first
        sortedMapIterator.remove();

        stateStaledErrorHandle();
    }

    /**
     * Handle state staled error by configured lenient option. If option is true, warning log only,
     * otherwise a {@link RuntimeException} will be thrown.
     */
    private void stateStaledErrorHandle() {
        // Skip the data if it's state is cleared because of state ttl.
        if (lenient) {
            LOG.warn(STATE_CLEARED_WARN_MSG);
        } else {
            throw new RuntimeException(STATE_CLEARED_WARN_MSG);
        }
    }

    private void emitRecordsWithRowNumber(
            SortedMap<RowData, Long> sortedMap,
            RowData sortKey,
            RowData inputRow,
            Collector<RowData> out)
            throws Exception {
        Iterator<Map.Entry<RowData, Long>> iterator = sortedMap.entrySet().iterator();
        long currentRank = 0L;
        RowData currentRow = null;
        boolean findsSortKey = false;
        while (iterator.hasNext() && isInRankEnd(currentRank)) {
            Map.Entry<RowData, Long> entry = iterator.next();
            RowData key = entry.getKey();
            if (!findsSortKey && key.equals(sortKey)) {
                currentRank += entry.getValue();
                currentRow = inputRow;
                findsSortKey = true;
            } else if (findsSortKey) {
                List<RowData> inputs = dataState.get(key);
                if (inputs == null) {
                    processStateStaled(iterator);
                } else {
                    int i = 0;
                    while (i < inputs.size() && isInRankEnd(currentRank)) {
                        RowData prevRow = inputs.get(i);
                        collectUpdateBefore(out, prevRow, currentRank);
                        collectUpdateAfter(out, currentRow, currentRank);
                        currentRow = prevRow;
                        currentRank += 1;
                        i++;
                    }
                }
            } else {
                currentRank += entry.getValue();
            }
        }
        if (isInRankEnd(currentRank)) {
            // there is no enough elements in Top-N, emit INSERT message for the new record.
            collectInsert(out, currentRow, currentRank);
        }
    }

    private void emitRecordsWithoutRowNumber(
            SortedMap<RowData, Long> sortedMap,
            RowData sortKey,
            RowData inputRow,
            Collector<RowData> out)
            throws Exception {
        Iterator<Map.Entry<RowData, Long>> iterator = sortedMap.entrySet().iterator();
        long curRank = 0L;
        boolean findsSortKey = false;
        RowData toCollect = null;
        RowData toDelete = null;
        while (iterator.hasNext() && isInRankEnd(curRank)) {
            Map.Entry<RowData, Long> entry = iterator.next();
            RowData key = entry.getKey();
            if (!findsSortKey && key.equals(sortKey)) {
                curRank += entry.getValue();
                if (isInRankRange(curRank)) {
                    toCollect = inputRow;
                }
                findsSortKey = true;
            } else if (findsSortKey) {
                List<RowData> inputs = dataState.get(key);
                if (inputs == null) {
                    processStateStaled(iterator);
                } else {
                    long count = entry.getValue();
                    // gets the rank of last record with same sortKey
                    long rankOfLastRecord = curRank + count;
                    // deletes the record if there is a record recently downgrades to Top-(N+1)
                    if (isInRankEnd(rankOfLastRecord)) {
                        curRank = rankOfLastRecord;
                    } else {
                        int index = Long.valueOf(rankEnd - curRank).intValue();
                        toDelete = inputs.get(index);
                        break;
                    }
                }
            } else {
                curRank += entry.getValue();
            }
        }
        if (toDelete != null) {
            collectDelete(out, inputRowSer.copy(toDelete));
        }
        if (toCollect != null) {
            collectInsert(out, inputRow);
        }
    }

    /**
     * Retract the input record and emit updated records. This works for outputting with row_number.
     *
     * @return true if the input record has been removed from {@link #dataState}.
     */
    private boolean retractRecordWithRowNumber(
            SortedMap<RowData, Long> sortedMap,
            RowData sortKey,
            RowData inputRow,
            Collector<RowData> out)
            throws Exception {
        Iterator<Map.Entry<RowData, Long>> iterator = sortedMap.entrySet().iterator();
        long currentRank = 0L;
        RowData prevRow = null;
        boolean findsSortKey = false;
        while (iterator.hasNext() && isInRankEnd(currentRank)) {
            Map.Entry<RowData, Long> entry = iterator.next();
            RowData key = entry.getKey();
            if (!findsSortKey && key.equals(sortKey)) {
                List<RowData> inputs = dataState.get(key);
                if (inputs == null) {
                    processStateStaled(iterator);
                } else {
                    Iterator<RowData> inputIter = inputs.iterator();
                    while (inputIter.hasNext() && isInRankEnd(currentRank)) {
                        RowData currentRow = inputIter.next();
                        if (!findsSortKey && equaliser.equals(currentRow, inputRow)) {
                            prevRow = currentRow;
                            findsSortKey = true;
                            inputIter.remove();
                        } else if (findsSortKey) {
                            collectUpdateBefore(out, prevRow, currentRank);
                            collectUpdateAfter(out, currentRow, currentRank);
                            prevRow = currentRow;
                        }
                        currentRank += 1;
                    }
                    if (inputs.isEmpty()) {
                        dataState.remove(key);
                    } else {
                        dataState.put(key, inputs);
                    }
                }
            } else if (findsSortKey) {
                List<RowData> inputs = dataState.get(key);
                if (inputs == null) {
                    processStateStaled(iterator);
                } else {
                    int i = 0;
                    while (i < inputs.size() && isInRankEnd(currentRank)) {
                        RowData currentRow = inputs.get(i);
                        collectUpdateBefore(out, prevRow, currentRank);
                        collectUpdateAfter(out, currentRow, currentRank);
                        prevRow = currentRow;
                        currentRank += 1;
                        i++;
                    }
                }
            } else {
                currentRank += entry.getValue();
            }
        }
        if (isInRankEnd(currentRank)) {
            if (!findsSortKey && null == prevRow) {
                stateStaledErrorHandle();
            } else {
                // there is no enough elements in Top-N, emit DELETE message for the retract record.
                collectDelete(out, prevRow, currentRank);
            }
        }

        return findsSortKey;
    }

    /**
     * Retract the input record and emit updated records. This works for outputting without
     * row_number.
     *
     * @return true if the input record has been removed from {@link #dataState}.
     */
    private boolean retractRecordWithoutRowNumber(
            SortedMap<RowData, Long> sortedMap,
            RowData sortKey,
            RowData inputRow,
            Collector<RowData> out)
            throws Exception {
        Iterator<Map.Entry<RowData, Long>> iterator = sortedMap.entrySet().iterator();
        long nextRank = 1L; // the next rank number, should be in the rank range
        boolean findsSortKey = false;
        while (iterator.hasNext() && isInRankEnd(nextRank)) {
            Map.Entry<RowData, Long> entry = iterator.next();
            RowData key = entry.getKey();
            if (!findsSortKey && key.equals(sortKey)) {
                List<RowData> inputs = dataState.get(key);
                if (inputs == null) {
                    processStateStaled(iterator);
                } else {
                    Iterator<RowData> inputIter = inputs.iterator();
                    while (inputIter.hasNext() && isInRankEnd(nextRank)) {
                        RowData prevRow = inputIter.next();
                        if (!findsSortKey && equaliser.equals(prevRow, inputRow)) {
                            collectDelete(out, prevRow, nextRank);
                            nextRank -= 1;
                            findsSortKey = true;
                            inputIter.remove();
                        } else if (findsSortKey) {
                            if (nextRank == rankEnd) {
                                collectInsert(out, prevRow, nextRank);
                            }
                        }
                        nextRank += 1;
                    }
                    if (inputs.isEmpty()) {
                        dataState.remove(key);
                    } else {
                        dataState.put(key, inputs);
                    }
                }
            } else if (findsSortKey) {
                long count = entry.getValue();
                // gets the rank of last record with same sortKey
                long rankOfLastRecord = nextRank + count - 1;
                if (rankOfLastRecord < rankEnd) {
                    nextRank = rankOfLastRecord + 1;
                } else {
                    // sends the record if there is a record recently upgrades to Top-N
                    int index = Long.valueOf(rankEnd - nextRank).intValue();
                    List<RowData> inputs = dataState.get(key);
                    if (inputs == null) {
                        processStateStaled(iterator);
                    } else {
                        RowData toAdd = inputs.get(index);
                        collectInsert(out, toAdd);
                        break;
                    }
                }
            } else {
                nextRank += entry.getValue();
            }
        }

        return findsSortKey;
    }
}
