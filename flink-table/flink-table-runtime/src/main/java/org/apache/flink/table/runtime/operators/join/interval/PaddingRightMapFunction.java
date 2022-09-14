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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/** Function performing right padding. */
@Internal
public class PaddingRightMapFunction
        implements MapFunction<RowData, RowData>, ResultTypeQueryable<RowData> {
    private static final long serialVersionUID = 1L;

    private final OuterJoinPaddingUtil paddingUtil;
    private final InternalTypeInfo<RowData> outputTypeInfo;

    public PaddingRightMapFunction(
            OuterJoinPaddingUtil paddingUtil, InternalTypeInfo<RowData> returnType) {
        this.paddingUtil = paddingUtil;
        this.outputTypeInfo = returnType;
    }

    @Override
    public RowData map(RowData value) {
        return paddingUtil.padRight(value);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return outputTypeInfo;
    }
}
