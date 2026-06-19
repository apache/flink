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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Function that used to define SQL time functions like LOCALTIME, CURRENT_TIME(these are all
 * dynamic functions in Calcite's {@link SqlStdOperatorTable}) in Flink, the difference from the
 * parent {@link SqlAbstractTimeFunction} is this function class be aware of whether it is used in
 * batch mode, if true it will act totally same as the parent {@link SqlAbstractTimeFunction}, but
 * will be a non-deterministic function if not in batch mode.
 */
@Internal
public class FlinkTimestampDynamicFunction extends SqlAbstractTimeFunction {

    protected final boolean isBatchMode;

    public FlinkTimestampDynamicFunction(
            String functionName, SqlTypeName returnTypeName, boolean isBatchMode) {
        super(functionName, returnTypeName);
        this.isBatchMode = isBatchMode;
    }

    @Override
    public boolean isDynamicFunction() {
        return isBatchMode && super.isDynamicFunction();
    }

    @Override
    public boolean isDeterministic() {
        // be a non-deterministic function in streaming mode
        return isBatchMode;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (!(obj instanceof FlinkTimestampDynamicFunction)) {
            return false;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        FlinkTimestampDynamicFunction other = (FlinkTimestampDynamicFunction) obj;
        return this.getName().equals(other.getName())
                && kind == other.kind
                && this.isBatchMode == other.isBatchMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, this.getName(), isBatchMode);
    }
}
