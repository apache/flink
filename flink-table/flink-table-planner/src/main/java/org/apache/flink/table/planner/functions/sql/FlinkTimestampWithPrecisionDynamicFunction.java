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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Function that used to define SQL time function like LOCALTIMESTAMP, CURRENT_TIMESTAMP, NOW() in
 * Flink, the function support configuring the return type and the * precision of return type.
 */
@Internal
public class FlinkTimestampWithPrecisionDynamicFunction extends FlinkTimestampDynamicFunction {
    /** function name for 'NOW()'. */
    public static final String NOW = "NOW";

    private final SqlTypeName returnTypeName;

    private final int precision;

    public FlinkTimestampWithPrecisionDynamicFunction(
            String name, SqlTypeName typeName, boolean isBatchMode, int precision) {
        super(name, typeName, isBatchMode);
        this.returnTypeName = typeName;
        this.precision = precision;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return opBinding.getTypeFactory().createSqlType(returnTypeName, precision);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (!(obj instanceof FlinkTimestampWithPrecisionDynamicFunction)) {
            return false;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        FlinkTimestampWithPrecisionDynamicFunction other =
                (FlinkTimestampWithPrecisionDynamicFunction) obj;
        return this.getName().equals(other.getName())
                && kind == other.kind
                && this.isBatchMode == other.isBatchMode
                && this.precision == other.precision
                && this.returnTypeName.equals(other.returnTypeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, this.getName(), isBatchMode, precision, returnTypeName);
    }
}
