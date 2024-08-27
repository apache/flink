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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The {@link SortPartitionOperator} sorts records of a partition on non-keyed data stream. It
 * ensures that all records within the same task are sorted in a user-defined order.
 *
 * <p>To sort the records, the record will be written to {@link ExternalSorter} directly. However,
 * if the record is sorted according to the selected key by {@link KeySelector}, the selected key
 * should also be written with the record to {@link ExternalSorter} to avoid repeated key
 * selections.
 *
 * @param <INPUT> The type of input record.
 */
@Internal
public class SortPartitionOperator<INPUT> extends AbstractStreamOperator<INPUT>
        implements OneInputStreamOperator<INPUT, INPUT>, BoundedOneInput {

    /** The type information of input records. */
    protected final TypeInformation<INPUT> inputType;

    /** The selector to create the sort key for records, which will be null if it's not used. */
    protected final KeySelector<INPUT, ?> sortFieldSelector;

    /** The order to sort records. */
    private final Order sortOrder;

    /**
     * The string field to indicate the sort key for records with tuple or pojo type, which will be
     * null if it's not used.
     */
    private final String stringSortField;

    /**
     * The int field to indicate the sort key for records with tuple type, which will be -1 if it's
     * not used.
     */
    private final int positionSortField;

    /** The sorter to sort record if the record is not sorted by {@link KeySelector}. */
    private PushSorter<INPUT> recordSorter = null;

    /** The sorter to sort record if the record is sorted by {@link KeySelector}. */
    private PushSorter<Tuple2<?, INPUT>> recordSorterForKeySelector = null;

    public SortPartitionOperator(
            TypeInformation<INPUT> inputType, int positionSortField, Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(positionSortField);
        this.positionSortField = positionSortField;
        this.stringSortField = null;
        this.sortFieldSelector = null;
        this.sortOrder = sortOrder;
    }

    public SortPartitionOperator(
            TypeInformation<INPUT> inputType, String stringSortField, Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(stringSortField);
        this.positionSortField = -1;
        this.stringSortField = stringSortField;
        this.sortFieldSelector = null;
        this.sortOrder = sortOrder;
    }

    public <K> SortPartitionOperator(
            TypeInformation<INPUT> inputType,
            KeySelector<INPUT, K> sortFieldSelector,
            Order sortOrder) {
        this.inputType = inputType;
        ensureFieldSortable(sortFieldSelector);
        this.positionSortField = -1;
        this.stringSortField = null;
        this.sortFieldSelector = sortFieldSelector;
        this.sortOrder = sortOrder;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<INPUT>> output) {
        super.setup(containingTask, config, output);
        ExecutionConfig executionConfig = containingTask.getEnvironment().getExecutionConfig();
        if (sortFieldSelector != null) {
            TypeInformation<Tuple2<?, INPUT>> sortTypeInfo =
                    Types.TUPLE(
                            TypeExtractor.getKeySelectorTypes(sortFieldSelector, inputType),
                            inputType);
            recordSorterForKeySelector =
                    getSorter(
                            sortTypeInfo.createSerializer(executionConfig),
                            ((CompositeType<Tuple2<?, INPUT>>) sortTypeInfo)
                                    .createComparator(
                                            getSortFieldIndex(),
                                            getSortOrderIndicator(),
                                            0,
                                            executionConfig),
                            containingTask);
        } else {
            recordSorter =
                    getSorter(
                            inputType.createSerializer(executionConfig),
                            ((CompositeType<INPUT>) inputType)
                                    .createComparator(
                                            getSortFieldIndex(),
                                            getSortOrderIndicator(),
                                            0,
                                            executionConfig),
                            containingTask);
        }
    }

    @Override
    public void processElement(StreamRecord<INPUT> element) throws Exception {
        if (sortFieldSelector != null) {
            recordSorterForKeySelector.writeRecord(
                    Tuple2.of(sortFieldSelector.getKey(element.getValue()), element.getValue()));
        } else {
            recordSorter.writeRecord(element.getValue());
        }
    }

    @Override
    public void endInput() throws Exception {
        TimestampedCollector<INPUT> outputCollector = new TimestampedCollector<>(output);
        if (sortFieldSelector != null) {
            recordSorterForKeySelector.finishReading();
            MutableObjectIterator<Tuple2<?, INPUT>> dataIterator =
                    recordSorterForKeySelector.getIterator();
            Tuple2<?, INPUT> record = dataIterator.next();
            while (record != null) {
                outputCollector.collect(record.f1);
                record = dataIterator.next();
            }
            recordSorterForKeySelector.close();
        } else {
            recordSorter.finishReading();
            MutableObjectIterator<INPUT> dataIterator = recordSorter.getIterator();
            INPUT record = dataIterator.next();
            while (record != null) {
                outputCollector.collect(record);
                record = dataIterator.next();
            }
            recordSorter.close();
        }
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().setOutputOnlyAfterEndOfStream(true).build();
    }

    /**
     * Get the sort field index for the sorted data.
     *
     * @return the sort field index.
     */
    private int[] getSortFieldIndex() {
        int[] sortFieldIndex = new int[1];
        if (positionSortField != -1) {
            sortFieldIndex[0] =
                    new Keys.ExpressionKeys<>(positionSortField, inputType)
                            .computeLogicalKeyPositions()[0];
        } else if (stringSortField != null) {
            sortFieldIndex[0] =
                    new Keys.ExpressionKeys<>(stringSortField, inputType)
                            .computeLogicalKeyPositions()[0];
        }
        return sortFieldIndex;
    }

    /**
     * Get the indicator for the sort order.
     *
     * @return sort order indicator.
     */
    private boolean[] getSortOrderIndicator() {
        boolean[] sortOrderIndicator = new boolean[1];
        sortOrderIndicator[0] = this.sortOrder == Order.ASCENDING;
        return sortOrderIndicator;
    }

    private void ensureFieldSortable(int field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException(
                    "The field " + field + " of input type " + inputType + " is not sortable.");
        }
    }

    private void ensureFieldSortable(String field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, inputType)) {
            throw new InvalidProgramException(
                    "The field " + field + " of input type " + inputType + " is not sortable.");
        }
    }

    private <K> void ensureFieldSortable(KeySelector<INPUT, K> keySelector) {
        TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, inputType);
        Keys.SelectorFunctionKeys<INPUT, K> sortKey =
                new Keys.SelectorFunctionKeys<>(keySelector, inputType, keyType);
        if (!sortKey.getKeyType().isSortKeyType()) {
            throw new InvalidProgramException("The key type " + keyType + " is not sortable.");
        }
    }

    private <TYPE> PushSorter<TYPE> getSorter(
            TypeSerializer<TYPE> typeSerializer,
            TypeComparator<TYPE> typeComparator,
            StreamTask<?, ?> streamTask) {
        ClassLoader userCodeClassLoader = streamTask.getUserCodeClassLoader();
        Configuration jobConfiguration = streamTask.getEnvironment().getJobConfiguration();
        double managedMemoryFraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        streamTask.getEnvironment().getJobConfiguration(),
                        streamTask.getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader);
        try {
            return ExternalSorter.newBuilder(
                            streamTask.getEnvironment().getMemoryManager(),
                            streamTask,
                            typeSerializer,
                            typeComparator,
                            streamTask.getExecutionConfig())
                    .memoryFraction(managedMemoryFraction)
                    .enableSpilling(
                            streamTask.getEnvironment().getIOManager(),
                            jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                    .maxNumFileHandles(jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                    .objectReuse(streamTask.getExecutionConfig().isObjectReuseEnabled())
                    .largeRecords(jobConfiguration.get(AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                    .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }
}
