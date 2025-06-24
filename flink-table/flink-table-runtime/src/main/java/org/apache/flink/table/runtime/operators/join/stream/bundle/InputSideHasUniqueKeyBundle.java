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

package org.apache.flink.table.runtime.operators.join.stream.bundle;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** For the case that input has uniqueKey which is not contained by joinKey. */
public class InputSideHasUniqueKeyBundle extends BufferBundle<Map<RowData, List<RowData>>> {

    @Override
    public int addRecord(RowData joinKey, RowData uniqueKey, RowData record) {
        bundle.computeIfAbsent(joinKey, k -> new HashMap<>())
                .computeIfAbsent(uniqueKey, k -> new ArrayList<>());
        if (!foldRecord(joinKey, uniqueKey, record)) {
            actualSize++;
            bundle.computeIfAbsent(joinKey, k -> new HashMap<>())
                    .computeIfAbsent(uniqueKey, key -> new ArrayList<>())
                    .add(record);
        }
        return ++count;
    }

    @Override
    public Map<RowData, List<RowData>> getRecords() {
        Map<RowData, List<RowData>> result = new HashMap<>();
        for (RowData joinKey : bundle.keySet()) {
            List<RowData> list = result.computeIfAbsent(joinKey, key -> new ArrayList<>());
            bundle.get(joinKey).values().stream().flatMap(Collection::stream).forEach(list::add);
        }
        return result;
    }

    @Override
    public Set<RowData> getJoinKeys() {
        return bundle.keySet();
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJoinKey(RowData joinKey) {
        return bundle.get(joinKey);
    }

    //
    // +--------------------------+----------------------------+----------------------------+
    // |   Before the last        |       Last record          |          Result            |
    // |--------------------------|----------------------------|----------------------------|
    // |    +I/+U                 |        +U/+I               |    Only keep the last      |
    // |                          |                            |       (+U/+I) record       |
    // |--------------------------|----------------------------|----------------------------|
    // |    -D/-U                 |        -D/-U               |    Only keep the last      |
    // |                          |                            |       (-D/-U) record       |
    // |--------------------------|----------------------------|----------------------------|
    // |    +I/+U                 |        -U/-D               |       Clear both           |
    // +--------------------------+----------------------------+----------------------------+

    /**
     * Folds the records in reverse order based on a specific rule. The rule is as above.
     *
     * <p>In this context, the symbols refer to the following RowKind values: accumulateMsg refers
     * to +I/+U which refers to {@link RowKind#INSERT}/{@link RowKind#UPDATE_AFTER}. retractMsg
     * refers to -U/-D which refers to {@link RowKind#UPDATE_BEFORE}/{@link RowKind#DELETE}.
     */
    private boolean foldRecord(RowData joinKey, RowData uniqueKey, RowData record) {
        List<RowData> list = bundle.get(joinKey).get(uniqueKey);
        boolean shouldFoldRecord = false;
        Optional<RowData> prevRecord =
                list.isEmpty() ? Optional.empty() : Optional.of(list.get(list.size() - 1));
        if (prevRecord.isPresent()) {
            RowData last = prevRecord.get();
            if (RowDataUtil.isAccumulateMsg(last)) {
                if (RowDataUtil.isRetractMsg(record)) {
                    shouldFoldRecord = true;
                }
                actualSize--;
                list.remove(list.size() - 1);
                if (list.isEmpty() && shouldFoldRecord) {
                    bundle.get(joinKey).remove(uniqueKey);
                    if (bundle.get(joinKey).isEmpty()) {
                        bundle.remove(joinKey);
                    }
                }
            } else if (RowDataUtil.isRetractMsg(record)) {
                // -D/-U -D/-U
                actualSize--;
                list.remove(list.size() - 1);
            }
        }
        return shouldFoldRecord;
    }
}
