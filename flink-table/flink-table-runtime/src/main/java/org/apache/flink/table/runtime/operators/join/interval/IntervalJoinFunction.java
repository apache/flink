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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NullAwareGetters;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.NullAwareJoinHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

/**
 * {@link IntervalJoinFunction} is a {@link RichFlatJoinFunction} wrappers a {@link
 * GeneratedJoinCondition}.
 */
public class IntervalJoinFunction extends RichFlatJoinFunction<RowData, RowData, RowData>
        implements ResultTypeQueryable<RowData> {

    private final InternalTypeInfo<RowData> returnTypeInfo;
    private final int[] nullFilterKeys;
    private final boolean nullSafe;
    private final boolean filterAllNulls;
    private GeneratedJoinCondition joinConditionCode;

    private transient JoinCondition joinCondition;
    private transient JoinedRowData reusedJoinRowData;
    private transient NullAwareGetters joinKey;

    public IntervalJoinFunction(
            GeneratedJoinCondition joinCondition,
            InternalTypeInfo<RowData> returnTypeInfo,
            boolean[] filterNulls) {
        this.joinConditionCode = joinCondition;
        this.returnTypeInfo = returnTypeInfo;
        this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
        this.nullSafe = nullFilterKeys.length == 0;
        this.filterAllNulls = nullFilterKeys.length == filterNulls.length;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.joinCondition =
                joinConditionCode.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinConditionCode = null;
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(openContext);
        this.reusedJoinRowData = new JoinedRowData();
    }

    @Override
    public void join(RowData first, RowData second, Collector<RowData> out) throws Exception {
        if (!nullSafe) { // is not null safe, return false if any null exists
            if (filterAllNulls ? joinKey.anyNull() : joinKey.anyNull(nullFilterKeys)) {
                // find null present, return false directly
                return;
            }
        }
        if (joinCondition.apply(first, second)) {
            out.collect(reusedJoinRowData.replace(first, second));
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return returnTypeInfo;
    }

    public void setJoinKey(RowData currentKey) {
        this.joinKey = (NullAwareGetters) currentKey;
    }
}
