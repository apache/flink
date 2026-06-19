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

import org.apache.calcite.sql.fun.SqlCurrentDateFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * The Flink CURRENT_DATE function differs from the parent {@link SqlCurrentDateFunction} which is
 * aware of whether it is used in batch mode, if true it will act totally same as the parent {@link
 * SqlCurrentDateFunction}, but will be a non-deterministic function if not in batch mode.
 */
@Internal
public class FlinkCurrentDateDynamicFunction extends SqlCurrentDateFunction {

    private final boolean isBatchMode;

    public FlinkCurrentDateDynamicFunction(boolean isBatchMode) {
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
        if (!(obj instanceof FlinkCurrentDateDynamicFunction)) {
            return false;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        FlinkCurrentDateDynamicFunction other = (FlinkCurrentDateDynamicFunction) obj;
        return this.getName().equals(other.getName())
                && kind == other.kind
                && this.isBatchMode == other.isBatchMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, this.getName(), isBatchMode);
    }
}
