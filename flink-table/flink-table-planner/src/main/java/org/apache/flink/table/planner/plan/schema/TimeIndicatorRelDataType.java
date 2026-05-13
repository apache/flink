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

package org.apache.flink.table.planner.plan.schema;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Creates a time indicator type for event-time or processing-time, but with similar properties as a
 * basic SQL type.
 */
public class TimeIndicatorRelDataType extends BasicSqlType {
    private final boolean isEventTime;
    private final BasicSqlType originalType;

    public TimeIndicatorRelDataType(
            RelDataTypeSystem typeSystem,
            BasicSqlType originalType,
            boolean nullable,
            boolean isEventTime) {
        super(typeSystem, originalType.getSqlTypeName(), nullable, originalType.getPrecision());
        this.isEventTime = isEventTime;
        this.originalType = originalType;
        computeDigest();
    }

    public boolean isEventTime() {
        return isEventTime;
    }

    public BasicSqlType getOriginalType() {
        return originalType;
    }

    public RelDataTypeSystem getTypeSystem() {
        return typeSystem;
    }

    @Override
    public int hashCode() {
        return super.hashCode()
                + 42; // we change the hash code to differentiate from regular timestamps
    }

    @Override
    public String toString() {
        // Calcite caches type instance by the type string representation in
        // org.apache.calcite.rel.type.RelDataTypeFactoryImpl, thus we use
        // unique name for each TimeIndicatorRelDataType
        final String typeNameStr =
                (typeName == SqlTypeName.TIMESTAMP) ? "TIMESTAMP(3)" : "TIMESTAMP_LTZ(3)";
        return typeNameStr + " " + (isEventTime ? "*ROWTIME*" : "*PROCTIME*");
    }

    @Override
    public void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(this);
    }
}
