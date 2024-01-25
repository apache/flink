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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * For the case that input has joinKey contains uniqueKey. The size of records in state is not
 * bigger than 1.
 */
public class JoinKeyContainsUniqueKeyBundle extends BufferBundle<List<RowData>> {

    @Override
    public int addRecord(RowData joinKey, @Nullable RowData uniqueKey, RowData record) {
        bundle.computeIfAbsent(joinKey, key -> new ArrayList<>());
        if (!foldRecord(joinKey, record)) {
            actualSize++;
            bundle.computeIfAbsent(joinKey, key -> new ArrayList<>()).add(record);
        }
        return ++count;
    }

    @Override
    public Map<RowData, List<RowData>> getRecords() throws Exception {
        return bundle;
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJoinKey(RowData joinKey) {
        throw new UnsupportedOperationException(
                "JoinKeyContainsUniqueKeyBundle do not support getRecordsWithJoinKey() function.");
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
     * <p>In this context, the symbols refer to the following RowKind values: "+I" refers to {@link
     * RowKind#INSERT}. "+U" refers to {@link RowKind#UPDATE_AFTER}. "-U" refers to {@link
     * RowKind#UPDATE_BEFORE}. "-D" refers to {@link RowKind#DELETE}.
     */
    private boolean foldRecord(RowData joinKey, RowData record) {
        List<RowData> list = bundle.get(joinKey);
        boolean shouldFoldRecord = false;

        Optional<RowData> lastElement =
                list.isEmpty() ? Optional.empty() : Optional.of(list.get(list.size() - 1));
        if (lastElement.isPresent()) {
            RowData last = lastElement.get();
            if (RowDataUtil.isAccumulateMsg(last)) {
                if (RowDataUtil.isRetractMsg(record)) {
                    shouldFoldRecord = true;
                }
                actualSize--;
                list.remove(list.size() - 1);
                if (list.isEmpty() && shouldFoldRecord) {
                    bundle.remove(joinKey);
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
