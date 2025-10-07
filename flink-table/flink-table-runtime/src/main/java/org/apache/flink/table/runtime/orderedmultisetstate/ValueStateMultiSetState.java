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

package org.apache.flink.table.runtime.orderedmultisetstate;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Simple implementation of {@link OrderedMultiSetState} based on plain {@code ValueState<List>}.
 */
class ValueStateMultiSetState implements OrderedMultiSetState<RowData> {

    private final ValueState<List<Tuple2<RowData, Long>>> valuesState;
    private final RecordEqualiser keyEqualiser;
    private final Function<RowData, RowData> keyExtractor;
    private final TimeSelector timeSelector;
    private List<Tuple2<RowData, Long>> cache;

    ValueStateMultiSetState(
            ValueState<List<Tuple2<RowData, Long>>> valuesState,
            RecordEqualiser keyEqualiser,
            Function<RowData, RowData> keyExtractor,
            TimeSelector timeSelector) {
        this.valuesState = valuesState;
        this.keyEqualiser = keyEqualiser;
        this.keyExtractor = keyExtractor;
        this.timeSelector = timeSelector;
    }

    public static ValueStateMultiSetState create(
            OrderedMultiSetStateContext p, RuntimeContext ctx) {
        //noinspection rawtypes,unchecked
        return new ValueStateMultiSetState(
                ctx.getState(
                        new ValueStateDescriptor<>(
                                "list",
                                new ListSerializer<>(
                                        new TupleSerializer(
                                                Tuple2.class,
                                                new TypeSerializer[] {
                                                    p.recordSerializer, LongSerializer.INSTANCE
                                                })))),
                p.generatedKeyEqualiser.newInstance(ctx.getUserCodeClassLoader()),
                p.keyExtractor,
                p.config.getTimeSelector());
    }

    @Override
    public SizeChangeInfo add(RowData row, long ts) throws Exception {
        normalizeRowKind(row);
        final Tuple2<RowData, Long> toAdd = Tuple2.of(row, timeSelector.getTimestamp(ts));
        final RowData key = asKey(row);
        final List<Tuple2<RowData, Long>> list = maybeReadState();
        final int oldSize = list.size();

        int idx = Integer.MIN_VALUE;
        int i = 0;
        for (Tuple2<RowData, Long> t : list) {
            if (keyEqualiser.equals(asKey(t.f0), key)) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            list.add(toAdd);
        } else {
            list.set(idx, toAdd);
        }
        valuesState.update(list);
        return new SizeChangeInfo(oldSize, list.size());
    }

    @Override
    public SizeChangeInfo append(RowData row, long timestamp) throws Exception {
        normalizeRowKind(row);
        List<Tuple2<RowData, Long>> values = maybeReadState();
        long sizeBefore = values.size();
        values.add(Tuple2.of(row, timeSelector.getTimestamp(timestamp)));
        long sizeAfter = values.size();
        valuesState.update(values);
        return new SizeChangeInfo(sizeBefore, sizeAfter);
    }

    @Override
    public Iterator<Tuple2<RowData, Long>> iterator() throws Exception {
        return maybeReadState().iterator();
    }

    @Override
    public Tuple3<RemovalResultType, Optional<RowData>, SizeChangeInfo> remove(RowData row)
            throws Exception {
        normalizeRowKind(row);
        final RowData key = asKey(row);
        final List<Tuple2<RowData, Long>> list = maybeReadState();
        final int oldSize = list.size();

        int dropIdx = Integer.MIN_VALUE;
        RowData last = null;
        int i = 0;
        for (Tuple2<RowData, Long> t : list) {
            if (keyEqualiser.equals(key, asKey(t.f0))) {
                dropIdx = i;
                break;
            } else {
                last = t.f0;
            }
            i++;
        }
        if (dropIdx >= 0) {
            list.remove(dropIdx);
            valuesState.update(list);
        }
        return toRemovalResult(new SizeChangeInfo(oldSize, list.size()), dropIdx, row, last);
    }

    @Override
    public void loadCache() throws IOException {
        cache = readState();
    }

    @Override
    public void clearCache() {
        cache = null;
    }

    private List<Tuple2<RowData, Long>> maybeReadState() throws IOException {
        if (cache != null) {
            return cache;
        }
        return readState();
    }

    private List<Tuple2<RowData, Long>> readState() throws IOException {
        List<Tuple2<RowData, Long>> value = valuesState.value();
        if (value == null) {
            value = new ArrayList<>();
        }
        return value;
    }

    @Override
    public void clear() {
        clearCache();
        valuesState.clear();
    }

    @Override
    public boolean isEmpty() throws IOException {
        List<Tuple2<RowData, Long>> list = cache == null ? valuesState.value() : cache;
        return list != null && list.isEmpty();
    }

    private RowData asKey(RowData row) {
        return keyExtractor.apply(row);
    }

    private static void normalizeRowKind(RowData row) {
        row.setRowKind(RowKind.INSERT);
    }

    private static Tuple3<RemovalResultType, Optional<RowData>, SizeChangeInfo> toRemovalResult(
            SizeChangeInfo sizeChangeInfo, int dropIdx, RowData row, RowData last) {
        if (sizeChangeInfo.wasEmpty()) {
            return Tuple3.of(RemovalResultType.NOTHING_REMOVED, Optional.empty(), sizeChangeInfo);
        } else if (sizeChangeInfo.isEmpty()) {
            return Tuple3.of(RemovalResultType.ALL_REMOVED, Optional.of(row), sizeChangeInfo);
        } else if (dropIdx + 1 == sizeChangeInfo.sizeBefore) {
            return Tuple3.of(
                    RemovalResultType.REMOVED_LAST_ADDED, Optional.of(last), sizeChangeInfo);
        } else {
            return Tuple3.of(RemovalResultType.REMOVED_OTHER, Optional.empty(), sizeChangeInfo);
        }
    }
}
