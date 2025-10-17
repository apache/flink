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
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.StateChangeInfo;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateContext;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.RowTypeUtils;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that maintains incoming records in state corresponding to the upsert keys and
 * generates an upsert view for the downstream operator.
 *
 * <ul>
 *   <li>Adds an insertion to state and emits it with updated {@link RowKind}.
 *   <li>Applies a deletion to state.
 *   <li>Emits a deletion with updated {@link RowKind} iff affects the last record or the state is
 *       empty afterward. A deletion to an already updated record is swallowed.
 * </ul>
 */
@Internal
public class SinkUpsertMaterializerV2 extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SinkUpsertMaterializerV2.class);

    private final SequencedMultiSetStateContext stateParameters;

    // Buffer of emitted insertions on which deletions will be applied first.
    // The row kind might be +I or +U and will be ignored when applying the deletion.
    private transient TimestampedCollector<RowData> collector;

    private transient SequencedMultiSetState<RowData> orderedMultiSetState;
    private final boolean hasUpsertKey;

    public SinkUpsertMaterializerV2(
            boolean hasUpsertKey, SequencedMultiSetStateContext stateParameters) {
        this.hasUpsertKey = hasUpsertKey;
        this.stateParameters = stateParameters;
    }

    @Override
    public void open() throws Exception {
        super.open();
        orderedMultiSetState =
                SequencedMultiSetState.create(
                        stateParameters,
                        getRuntimeContext(),
                        getKeyedStateStore().getBackendTypeIdentifier());
        collector = new TimestampedCollector<>(output);
        LOG.info("Opened {} with upsert key: {}", this.getClass().getSimpleName(), hasUpsertKey);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData row = element.getValue();
        final long timestamp = element.getTimestamp();

        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (hasUpsertKey) {
                    collect(row, orderedMultiSetState.add(row, timestamp).wasEmpty());
                } else {
                    collect(row, orderedMultiSetState.append(row, timestamp).wasEmpty());
                }
                break;

            case UPDATE_BEFORE:
            case DELETE:
                StateChangeInfo<RowData> removalResult = orderedMultiSetState.remove(row);
                switch (removalResult.getChangeType()) {
                    case REMOVAL_OTHER:
                        // do nothing;
                        break;
                    case REMOVAL_NOT_FOUND:
                        LOG.warn("Not found record to retract"); // not logging the record due for
                        // security
                        break;
                    case REMOVAL_ALL:
                        collect(removalResult.getPayload().get(), RowKind.DELETE);
                        break;
                    case REMOVAL_LAST_ADDED:
                        collect(removalResult.getPayload().get(), RowKind.UPDATE_AFTER);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unexpected removal result type: " + removalResult.getChangeType());
                }
        }
    }

    private void collect(RowData row, boolean notExisted) {
        collect(row, getRowKind(notExisted));
    }

    private RowKind getRowKind(boolean notExisted) {
        return notExisted ? RowKind.INSERT : RowKind.UPDATE_AFTER;
    }

    private void collect(RowData row, RowKind withKind) {
        RowKind orig = row.getRowKind();
        row.setRowKind(withKind);
        collector.collect(row);
        row.setRowKind(orig);
    }

    public static SinkUpsertMaterializerV2 create(
            RowType physicalRowType,
            GeneratedRecordEqualiser rowEqualiser,
            GeneratedRecordEqualiser upsertKeyEqualiser,
            GeneratedHashFunction rowHashFunction,
            GeneratedHashFunction upsertKeyHashFunction,
            int[] inputUpsertKey,
            SequencedMultiSetStateConfig stateSettings) {

        boolean hasUpsertKey = inputUpsertKey != null && inputUpsertKey.length > 0;

        return new SinkUpsertMaterializerV2(
                hasUpsertKey,
                new SequencedMultiSetStateContext(
                        checkNotNull(
                                hasUpsertKey
                                        ? InternalSerializers.create(
                                                RowTypeUtils.projectRowType(
                                                        physicalRowType, inputUpsertKey))
                                        : InternalSerializers.create(physicalRowType)),
                        checkNotNull(hasUpsertKey ? upsertKeyEqualiser : rowEqualiser),
                        checkNotNull(hasUpsertKey ? upsertKeyHashFunction : rowHashFunction),
                        InternalSerializers.create(physicalRowType),
                        row ->
                                hasUpsertKey
                                        ? ProjectedRowData.from(inputUpsertKey, true)
                                                .replaceRow(row)
                                        : row,
                        stateSettings));
    }
}
