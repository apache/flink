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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateContext;
import org.apache.flink.table.runtime.sequencedmultisetstate.TimeSelector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.ALL_REMOVED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.NOTHING_REMOVED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.REMOVED_LAST_ADDED;
import static org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.RemovalResultType.REMOVED_OTHER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements an ordered multi-set state backend using Flink's state primitives. It
 * maintains the insertion order of elements and supports operations such as adding, appending, and
 * removing elements. The state is backed by Flink's `MapState` and `ValueState` to store and manage
 * the relationships between rows and sequence numbers (SQNs).
 *
 * <p>Key features of this state implementation:
 *
 * <ul>
 *   <li>Maintains insertion order of elements using a doubly-linked list structure.
 *   <li>Supports both normal set semantics (replacing existing elements) and multi-set semantics
 *       (allowing duplicates).
 *   <li>Efficiently tracks the highest sequence number and links between elements for fast
 *       traversal and updates.
 *   <li>Provides methods to add, append, and remove elements with appropriate handling of state
 *       transitions.
 * </ul>
 *
 * <p>Note: This implementation is marked as {@code @Internal} and is intended for internal use
 * within Flink. It may be subject to changes in future versions.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Use the {@link #add(RowData, long)} method to add an element, replacing any existing
 *       matching element.
 *   <li>Use the {@link #append(RowData, long)} method to append an element, allowing duplicates.
 *   <li>Use the {@link #remove(RowData)} method to remove an element, with detailed removal result
 *       types.
 * </ul>
 *
 * @see SequencedMultiSetState
 * @see org.apache.flink.api.common.state.MapState
 * @see org.apache.flink.api.common.state.ValueState
 */
@Internal
public class LinkedMultiSetState implements SequencedMultiSetState<RowData> {

    // maps rows to SQNs (single SQN per RowData in case of upsert key; last SQN otherwise)
    private final MapState<RowDataKey, Long> rowToSqnState;
    // maps SQNs to Nodes, which comprise a doubly-linked list
    private final MapState<Long, Node> sqnToNodeState;
    // highest sequence number; also latest emitted downstream
    private final ValueState<Tuple2<Long, Long>> highestSqnAndSizeState;

    private final RecordEqualiser keyEqualiser;
    private final HashFunction keyHashFunction;
    private final Function<RowData, RowData> keyExtractor;
    private final TimeSelector timeSelector;

    private LinkedMultiSetState(
            MapState<RowDataKey, Long> rowToSqnState,
            MapState<Long, Node> sqnToNodeState,
            ValueState<Tuple2<Long, Long>> highestSqnAndSizeState,
            RecordEqualiser keyEqualiser,
            HashFunction keyHashFunction,
            Function<RowData, RowData> keyExtractor,
            TimeSelector timeSelector) {
        this.rowToSqnState = checkNotNull(rowToSqnState);
        this.sqnToNodeState = checkNotNull(sqnToNodeState);
        this.highestSqnAndSizeState = checkNotNull(highestSqnAndSizeState);
        this.keyEqualiser = checkNotNull(keyEqualiser);
        this.keyHashFunction = checkNotNull(keyHashFunction);
        this.keyExtractor = keyExtractor;
        this.timeSelector = timeSelector;
    }

    public static LinkedMultiSetState create(SequencedMultiSetStateContext p, RuntimeContext ctx) {

        RecordEqualiser keyEqualiser =
                p.generatedKeyEqualiser.newInstance(ctx.getUserCodeClassLoader());
        HashFunction keyHashFunction =
                p.generatedKeyHashFunction.newInstance(ctx.getUserCodeClassLoader());

        MapState<RowDataKey, Long> rowToSqnState =
                ctx.getMapState(
                        new MapStateDescriptor<>(
                                "rowToSqnState",
                                new RowDataKeySerializer(
                                        p.keySerializer,
                                        keyEqualiser,
                                        keyHashFunction,
                                        p.generatedKeyEqualiser,
                                        p.generatedKeyHashFunction),
                                LongSerializer.INSTANCE));
        MapState<Long, Node> sqnToNodeState =
                ctx.getMapState(
                        new MapStateDescriptor<>(
                                "sqnToNodeState",
                                LongSerializer.INSTANCE,
                                new NodeSerializer(p.recordSerializer)));

        //noinspection rawtypes,unchecked
        ValueState<Tuple2<Long, Long>> highestSqnState =
                ctx.getState(
                        new ValueStateDescriptor<Tuple2<Long, Long>>(
                                "highestSqnState",
                                new TupleSerializer(
                                        Tuple2.class,
                                        new TypeSerializer[] {
                                            LongSerializer.INSTANCE, LongSerializer.INSTANCE
                                        })));

        return new LinkedMultiSetState(
                rowToSqnState,
                sqnToNodeState,
                highestSqnState,
                keyEqualiser,
                keyHashFunction,
                p.keyExtractor,
                p.config.getTimeSelector());
    }

    /**
     * Add row, replacing any matching existing ones.
     *
     * @return {@link
     *     org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.SizeChangeInfo}
     *     representing the result of the operation
     */
    @Override
    public SizeChangeInfo add(RowData row, long timestamp) throws Exception {
        final RowDataKey key = toKey(row);
        final Tuple2<Long, Long> highSqnAndSize = highestSqnAndSizeState.value();
        final Long highSqn = highSqnAndSize == null ? null : highSqnAndSize.f0;
        final long oldSize = highSqnAndSize == null ? 0 : highSqnAndSize.f1;
        final Long rowSqn = rowToSqnState.get(key);
        final boolean isNewRowKey = rowSqn == null; // it's a 1st such record 'row'
        final boolean isNewContextKey = highSqn == null; // 1st a record for current context key

        final Long oldSqn = isNewRowKey ? null : rowSqn;
        final long newSqn = isNewRowKey ? (isNewContextKey ? 0 : highSqn + 1) : oldSqn;
        final long newSize = isNewContextKey ? 1 : (isNewRowKey ? oldSize + 1 : oldSize);

        timestamp = timeSelector.getTimestamp(timestamp);

        sqnToNodeState.put(
                newSqn,
                isNewRowKey
                        ? new Node(row, newSqn, highSqn, null, null, timestamp)
                        : sqnToNodeState.get(oldSqn).withRow(row, timestamp));
        highestSqnAndSizeState.update(Tuple2.of(newSqn, newSize));
        if (isNewRowKey) {
            rowToSqnState.put(key, newSqn);
            if (!isNewContextKey) {
                sqnToNodeState.put(highSqn, sqnToNodeState.get(highSqn).withNext(newSqn));
            }
        }
        return new SizeChangeInfo(oldSize, newSize);
    }

    @Override
    public SizeChangeInfo append(RowData row, long timestamp) throws Exception {
        final RowDataKey key = toKey(row);
        final Tuple2<Long, Long> highSqnAndSize = highestSqnAndSizeState.value();
        final Long highSqn = highSqnAndSize == null ? null : highSqnAndSize.f0;
        final long oldSize = highSqnAndSize == null ? 0 : highSqnAndSize.f1;
        final boolean existed = highSqn != null;
        final long newSqn = (existed ? highSqn + 1 : 0);
        final Node newNode =
                new Node(
                        row,
                        newSqn,
                        highSqn, /*next*/
                        null, /*nextForRecord*/
                        null,
                        timeSelector.getTimestamp(timestamp));
        final long newSize = oldSize + 1;

        Long rowSqn = existed ? rowToSqnState.get(key) : null;
        if (rowSqn != null) {
            sqnToNodeState.put(rowSqn, sqnToNodeState.get(rowSqn).withNextForRecord(newSqn));
        }
        rowToSqnState.put(key, newSqn);
        highestSqnAndSizeState.update(Tuple2.of(newSqn, newSize));
        sqnToNodeState.put(newSqn, newNode);
        if (existed) {
            sqnToNodeState.put(highSqn, sqnToNodeState.get(highSqn).withNext(newSqn));
        }
        return new SizeChangeInfo(oldSize, newSize);
    }

    @Override
    public Tuple3<RemovalResultType, Optional<RowData>, SizeChangeInfo> remove(RowData row)
            throws Exception {
        final RowDataKey key = toKey(row);
        final Long rowSqn = rowToSqnState.get(key);
        final Tuple2<Long, Long> highSqnStateAndSize = highestSqnAndSizeState.value();
        final long oldSize = highSqnStateAndSize == null ? 0L : highSqnStateAndSize.f1;
        if (rowSqn == null) {
            return toRemovalResult(NOTHING_REMOVED, null, oldSize);
        }
        final Node node = sqnToNodeState.get(rowSqn);

        final Node prev = removeNode(node, key, highSqnStateAndSize);

        if (node.isHighestSqn()) {
            if (prev == null) {
                return toRemovalResult(ALL_REMOVED, row, oldSize);
            } else {
                return toRemovalResult(REMOVED_LAST_ADDED, prev.row, oldSize);
            }
        } else {
            return toRemovalResult(REMOVED_OTHER, null, oldSize);
        }
    }

    @Override
    public void clear() {
        clearCache();
        sqnToNodeState.clear();
        highestSqnAndSizeState.clear();
        rowToSqnState.clear();
    }

    @Override
    public void loadCache() {}

    @Override
    public void clearCache() {}

    private Node removeNode(Node node, RowDataKey key, Tuple2<Long, Long> highSqnStateAndSize)
            throws Exception {

        if (node.isLowestSqn() && node.isHighestSqn()) {
            // fast track: if last record for PK then cleanup everything and return
            clear();
            return null;
        }

        sqnToNodeState.remove(node.getSqn());
        highestSqnAndSizeState.update(
                Tuple2.of(
                        node.isHighestSqn() ? node.prevSqn : highSqnStateAndSize.f0,
                        highSqnStateAndSize.f1 - 1));
        if (node.isLastForRecord()) {
            rowToSqnState.remove(key);
        } else {
            rowToSqnState.put(key, node.nextSqnForRecord);
        }
        // link prev node to next
        Node prev = null;
        if (node.hasPrev()) {
            prev = sqnToNodeState.get(node.prevSqn).withNext(node.nextSqn);
            sqnToNodeState.put(node.prevSqn, prev);
        }
        // link next node to prev
        if (node.hasNext()) {
            sqnToNodeState.put(
                    node.nextSqn, sqnToNodeState.get(node.nextSqn).withPrev(node.prevSqn));
        }
        return prev;
    }

    @Override
    public Iterator<Tuple2<RowData, Long>> iterator() throws Exception {
        // this can be implemented more efficiently
        // however, the expected use case is to migrate all the values either to or from the memory
        // state backend, so loading all into memory seems fine
        List<Node> list = new ArrayList<>();
        for (Node node : sqnToNodeState.values()) {
            list.add(node);
        }
        list.sort(Comparator.comparingLong(Node::getSqn));
        return list.stream().map(node -> Tuple2.of(node.row, node.timestamp)).iterator();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return highestSqnAndSizeState.value() == null;
    }

    private RowDataKey toKey(RowData row0) {
        return RowDataKey.toKey(keyExtractor.apply(row0), keyEqualiser, keyHashFunction);
    }

    private static Tuple3<RemovalResultType, Optional<RowData>, SizeChangeInfo> toRemovalResult(
            RemovalResultType type, @Nullable RowData row, long oldSize) {
        return Tuple3.of(
                type,
                Optional.ofNullable(row),
                new SizeChangeInfo(oldSize, type == NOTHING_REMOVED ? oldSize : oldSize - 1));
    }
}
