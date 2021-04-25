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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NullAwareGetters;
import org.apache.flink.table.runtime.generated.JoinCondition;

/** Utility to take null filters into consideration when apply join condition. */
public class JoinConditionWithNullFilters extends AbstractRichFunction implements JoinCondition {

    private final JoinCondition backingJoinCondition;

    /** Should filter null keys. */
    private final int[] nullFilterKeys;

    /** No keys need to filter null. */
    private final boolean nullSafe;

    /** Filter null to all keys. */
    private final boolean filterAllNulls;

    private NullAwareGetters joinKey;

    public JoinConditionWithNullFilters(
            JoinCondition backingJoinCondition, boolean[] filterNullKeys) {
        this.backingJoinCondition = backingJoinCondition;
        this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNullKeys);
        this.nullSafe = nullFilterKeys.length == 0;
        this.filterAllNulls = nullFilterKeys.length == filterNullKeys.length;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        backingJoinCondition.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        backingJoinCondition.close();
    }

    @Override
    public boolean apply(RowData left, RowData right) {
        if (!nullSafe) { // is not null safe, return false if any null exists
            // key is always BinaryRowData
            if (filterAllNulls ? joinKey.anyNull() : joinKey.anyNull(nullFilterKeys)) {
                // find null present, return false directly
                return false;
            }
        }
        // test condition
        return backingJoinCondition.apply(left, right);
    }

    public void setCurrentJoinKey(NullAwareGetters joinKey) {
        this.joinKey = joinKey;
    }
}
