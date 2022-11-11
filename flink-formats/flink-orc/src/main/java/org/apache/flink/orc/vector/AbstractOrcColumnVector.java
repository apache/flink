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

package org.apache.flink.orc.vector;

import org.apache.flink.orc.TimestampUtil;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;

/** This column vector is used to adapt hive's ColumnVector to Flink's ColumnVector. */
public abstract class AbstractOrcColumnVector
        implements org.apache.flink.table.data.columnar.vector.ColumnVector {

    private ColumnVector vector;

    AbstractOrcColumnVector(ColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public boolean isNullAt(int i) {
        return !vector.noNulls && vector.isNull[vector.isRepeating ? 0 : i];
    }

    public static org.apache.flink.table.data.columnar.vector.ColumnVector createFlinkVector(
            ColumnVector vector, LogicalType logicalType) {
        if (vector instanceof LongColumnVector) {
            if (logicalType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return new OrcLegacyTimestampColumnVector((LongColumnVector) vector);
            } else {
                return new OrcLongColumnVector((LongColumnVector) vector);
            }
        } else if (vector instanceof DoubleColumnVector) {
            return new OrcDoubleColumnVector((DoubleColumnVector) vector);
        } else if (vector instanceof BytesColumnVector) {
            return new OrcBytesColumnVector((BytesColumnVector) vector);
        } else if (vector instanceof DecimalColumnVector) {
            return new OrcDecimalColumnVector((DecimalColumnVector) vector);
        } else if (TimestampUtil.isHiveTimestampColumnVector(vector)) {
            return new OrcTimestampColumnVector(vector);
        } else if (vector instanceof ListColumnVector) {
            return new OrcArrayColumnVector((ListColumnVector) vector, (ArrayType) logicalType);
        } else if (vector instanceof StructColumnVector) {
            return new OrcRowColumnVector((StructColumnVector) vector, (RowType) logicalType);
        } else if (vector instanceof MapColumnVector) {
            return new OrcMapColumnVector((MapColumnVector) vector, (MapType) logicalType);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported vector: " + vector.getClass().getName());
        }
    }
}
