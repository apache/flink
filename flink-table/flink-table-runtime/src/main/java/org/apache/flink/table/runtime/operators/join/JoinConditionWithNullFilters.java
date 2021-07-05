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

import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NullAwareGetters;
import org.apache.flink.table.runtime.generated.JoinCondition;

/** Utility to take null filters into consideration when apply join condition. */
public class JoinConditionWithNullFilters extends WrappingFunction<JoinCondition>
        implements JoinCondition {

    private static final long serialVersionUID = 1L;

    /** Should filter null keys. */
    private final int[] nullFilterKeys;

    /** No keys need to filter null. */
    private final boolean nullSafe;

    /** Filter null to all keys. */
    private final boolean filterAllNulls;

    private final KeyContext keyContext;

    public JoinConditionWithNullFilters(
            JoinCondition backingJoinCondition, boolean[] filterNullKeys, KeyContext keyContext) {
        super(backingJoinCondition);
        this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNullKeys);
        this.nullSafe = nullFilterKeys.length == 0;
        this.filterAllNulls = nullFilterKeys.length == filterNullKeys.length;
        this.keyContext = keyContext;
    }

    @Override
    public boolean apply(RowData left, RowData right) {
        if (!nullSafe) { // is not null safe, return false if any null exists
            // key is always BinaryRowData
            NullAwareGetters joinKey = (NullAwareGetters) keyContext.getCurrentKey();
            if (filterAllNulls ? joinKey.anyNull() : joinKey.anyNull(nullFilterKeys)) {
                // find null present, return false directly
                return false;
            }
        }
        // test condition
        return wrappedFunction.apply(left, right);
    }
}
