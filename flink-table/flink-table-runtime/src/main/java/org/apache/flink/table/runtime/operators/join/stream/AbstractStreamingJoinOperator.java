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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Iterator;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = -376944622236540545L;

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final GeneratedJoinCondition generatedJoinCondition;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;

    protected final JoinInputSideSpec leftInputSideSpec;
    protected final JoinInputSideSpec rightInputSideSpec;

    private final boolean[] filterNullKeys;

    protected final long leftStateRetentionTime;
    protected final long rightStateRetentionTime;

    protected transient JoinConditionWithNullFilters joinCondition;
    protected transient TimestampedCollector<RowData> collector;

    public AbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.leftStateRetentionTime = leftStateRetentionTime;
        this.rightStateRetentionTime = rightStateRetentionTime;
        this.filterNullKeys = filterNullKeys;
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    /** Wrap the passed iterator with filter and mapper. */
    private static <I, O> Iterator<O> collect(
            final Iterator<I> delegate, Function<I, Boolean> matcher, Function<I, O> mapper) {
        return new Iterator<O>() {
            private I next;

            @Override
            public boolean hasNext() {
                advance();
                return next != null;
            }

            @Override
            public O next() {
                checkState(hasNext());
                I tmp = next;
                next = null;
                return mapper.apply(tmp);
            }

            private void advance() {
                while (next == null && delegate.hasNext()) {
                    I record = delegate.next();
                    if (matcher.apply(record)) {
                        next = record;
                    }
                }
            }
        };
    }

    /** Creates an {@link Iterator} over the records associated to the input row. */
    protected static Iterator<OuterRecord> iterator(
            RowData input,
            boolean inputIsLeft,
            JoinRecordStateView otherSideStateView,
            JoinCondition condition)
            throws Exception {
        if (otherSideStateView instanceof OuterJoinRecordStateView) {
            return collect(
                    ((OuterJoinRecordStateView) otherSideStateView)
                            .getRecordsAndNumOfAssociations()
                            .iterator(),
                    record ->
                            inputIsLeft
                                    ? condition.apply(input, record.f0)
                                    : condition.apply(record.f0, input),
                    record -> new OuterRecord(record.f0, record.f1));
        } else {
            return collect(
                    otherSideStateView.getRecords().iterator(),
                    record ->
                            inputIsLeft
                                    ? condition.apply(input, record)
                                    : condition.apply(record, input),
                    // use -1 as the default number of associations
                    record -> new OuterRecord(record, -1));
        }
    }

    /**
     * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
     * numOfAssociations} represents the number of associated records in the other side. It is used
     * when the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
     * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid
     * recompute the associated numbers every time.
     *
     * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
     * numOfAssociations} will always be {@code -1}.
     */
    protected static final class OuterRecord {
        public final RowData record;
        public final int numOfAssociations;

        private OuterRecord(RowData record, int numOfAssociations) {
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }
    }
}
