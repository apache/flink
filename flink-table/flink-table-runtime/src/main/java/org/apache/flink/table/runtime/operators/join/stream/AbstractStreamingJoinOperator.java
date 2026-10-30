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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

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

    // whether the input may update a record without retracting it first (upsert), so that the
    // record it replaces has to be looked up in state
    private final boolean leftIsUpsertOrUnknown;
    private final boolean rightIsUpsertOrUnknown;
    // whether a DELETE of the input may contain only the unique key, so that the full record has
    // to be looked up in state to evaluate a non-equi condition
    private final boolean leftHasKeyOnlyDeletesOrUnknown;
    private final boolean rightHasKeyOnlyDeletesOrUnknown;
    // whether the join condition has a non-equi part, e.g. A JOIN B ON A.k = B.k AND A.v > B.v
    protected final boolean hasNonEquiCondition;

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
            @Nullable RuntimeChangelogMode leftInputChangelogMode,
            @Nullable RuntimeChangelogMode rightInputChangelogMode,
            boolean hasNonEquiCondition,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.leftIsUpsertOrUnknown = isUpsertOrUnknown(leftInputChangelogMode);
        this.rightIsUpsertOrUnknown = isUpsertOrUnknown(rightInputChangelogMode);
        this.leftHasKeyOnlyDeletesOrUnknown = hasKeyOnlyDeletesOrUnknown(leftInputChangelogMode);
        this.rightHasKeyOnlyDeletesOrUnknown = hasKeyOnlyDeletesOrUnknown(rightInputChangelogMode);
        this.hasNonEquiCondition = hasNonEquiCondition;
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

    /**
     * Returns the stored record that the given record replaces, or null if there is none. An upsert
     * input updates a record by sending only the new version, without retracting the old one.
     *
     * <p>The lookup only happens if it can change the result.
     */
    protected @Nullable RowData replacedRecord(
            RowData record, JoinRecordStateView stateView, boolean isLeft, boolean otherIsOuter)
            throws Exception {
        final boolean inputIsUpsertOrUnknown =
                isLeft ? leftIsUpsertOrUnknown : rightIsUpsertOrUnknown;
        // with at most one record per join key, the count of the other side is right without it
        final boolean mayCountTwice = otherIsOuter && !joinKeyContainsUniqueKey(isLeft);
        final boolean mayMatchDifferently = hasNonEquiCondition;
        if (!inputIsUpsertOrUnknown || !(mayCountTwice || mayMatchDifferently)) {
            return null;
        }
        return stateView.getRecord(record);
    }

    /**
     * Returns the full record to retract.
     *
     * <p>A DELETE may contain only the unique key, e.g. from an upsert source, but a non-equi
     * condition needs all fields. In that case, the stored record with the same unique key is
     * returned, or the given retraction if nothing is stored.
     */
    protected RowData retractedRecord(
            RowData retraction,
            RowKind retractionKind,
            JoinRecordStateView stateView,
            boolean isLeft)
            throws Exception {
        final boolean inputHasKeyOnlyDeletesOrUnknown =
                isLeft ? leftHasKeyOnlyDeletesOrUnknown : rightHasKeyOnlyDeletesOrUnknown;
        final boolean mayBeKeyOnlyDelete =
                retractionKind == RowKind.DELETE
                        && inputHasKeyOnlyDeletesOrUnknown
                        && hasUniqueKey(isLeft);
        if (!mayBeKeyOnlyDelete || !hasNonEquiCondition) {
            return retraction;
        }
        final RowData storedRecord = stateView.getRecord(retraction);
        return storedRecord != null ? storedRecord : retraction;
    }

    protected boolean matches(RowData inputRecord, RowData otherRecord, boolean inputIsLeft) {
        return inputIsLeft
                ? joinCondition.apply(inputRecord, otherRecord)
                : joinCondition.apply(otherRecord, inputRecord);
    }

    protected boolean joinKeyContainsUniqueKey(boolean isLeft) {
        return (isLeft ? leftInputSideSpec : rightInputSideSpec).joinKeyContainsUniqueKey();
    }

    private boolean hasUniqueKey(boolean isLeft) {
        return (isLeft ? leftInputSideSpec : rightInputSideSpec).hasUniqueKey();
    }

    /** Returns whether the input may update a record without retracting it first. */
    private static boolean isUpsertOrUnknown(@Nullable RuntimeChangelogMode inputChangelogMode) {
        // compiled plans before Flink 2.4 do not contain the changelog mode, and handling
        // their inputs as upsert is always correct
        if (inputChangelogMode == null) {
            return true;
        }
        final ChangelogMode changelogMode = inputChangelogMode.deserialize();
        return changelogMode.contains(RowKind.UPDATE_AFTER)
                && !changelogMode.contains(RowKind.UPDATE_BEFORE);
    }

    /** Returns whether a DELETE of the input may contain only the unique key. */
    private static boolean hasKeyOnlyDeletesOrUnknown(
            @Nullable RuntimeChangelogMode inputChangelogMode) {
        // compiled plans before Flink 2.4 do not contain the changelog mode, and handling
        // their deletes as key-only is always correct
        return inputChangelogMode == null || inputChangelogMode.deserialize().keyOnlyDeletes();
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
