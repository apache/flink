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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.flink.table.utils.DateTimeUtils.toInternal;

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

    /** Create flink vector by hive vector from constant. */
    public static org.apache.flink.table.data.columnar.vector.ColumnVector
            createFlinkVectorFromConstant(LogicalType type, Object value, int batchSize) {
        return createFlinkVector(createHiveVectorFromConstant(type, value, batchSize), type);
    }

    /**
     * Create a orc vector from partition spec value. See hive {@code
     * VectorizedRowBatchCtx#addPartitionColsToBatch}.
     */
    private static ColumnVector createHiveVectorFromConstant(
            LogicalType type, Object value, int batchSize) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return createBytesVector(batchSize, value);
            case BOOLEAN:
                return createLongVector(batchSize, (Boolean) value ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return createLongVector(batchSize, value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return createDecimalVector(
                        batchSize, decimalType.getPrecision(), decimalType.getScale(), value);
            case FLOAT:
            case DOUBLE:
                return createDoubleVector(batchSize, value);
            case DATE:
                if (value instanceof LocalDate) {
                    value = Date.valueOf((LocalDate) value);
                }
                return createLongVector(batchSize, toInternal((Date) value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampUtil.createVectorFromConstant(batchSize, value);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static LongColumnVector createLongVector(int batchSize, Object value) {
        LongColumnVector lcv = new LongColumnVector(batchSize);
        if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
        } else {
            lcv.fill(((Number) value).longValue());
            lcv.isNull[0] = false;
        }
        return lcv;
    }

    private static BytesColumnVector createBytesVector(int batchSize, Object value) {
        BytesColumnVector bcv = new BytesColumnVector(batchSize);
        if (value == null) {
            bcv.noNulls = false;
            bcv.isNull[0] = true;
            bcv.isRepeating = true;
        } else {
            byte[] bytes =
                    value instanceof byte[]
                            ? (byte[]) value
                            : value.toString().getBytes(StandardCharsets.UTF_8);
            bcv.initBuffer(bytes.length);
            bcv.fill(bytes);
            bcv.isNull[0] = false;
        }
        return bcv;
    }

    private static DecimalColumnVector createDecimalVector(
            int batchSize, int precision, int scale, Object value) {
        DecimalColumnVector dv = new DecimalColumnVector(batchSize, precision, scale);
        if (value == null) {
            dv.noNulls = false;
            dv.isNull[0] = true;
            dv.isRepeating = true;
        } else {
            dv.set(
                    0,
                    value instanceof HiveDecimal
                            ? (HiveDecimal) value
                            : HiveDecimal.create((BigDecimal) value));
            dv.isRepeating = true;
            dv.isNull[0] = false;
        }
        return dv;
    }

    private static DoubleColumnVector createDoubleVector(int batchSize, Object value) {
        DoubleColumnVector dcv = new DoubleColumnVector(batchSize);
        if (value == null) {
            dcv.noNulls = false;
            dcv.isNull[0] = true;
            dcv.isRepeating = true;
        } else {
            dcv.fill(((Number) value).doubleValue());
            dcv.isNull[0] = false;
        }
        return dcv;
    }
}
