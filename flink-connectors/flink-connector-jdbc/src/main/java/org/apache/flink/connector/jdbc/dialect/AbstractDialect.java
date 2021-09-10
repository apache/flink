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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;

import java.util.List;

public abstract class AbstractDialect implements JdbcDialect {

    @Override
    public void validate(DataType dataType) throws ValidationException {
        if (!(dataType.getLogicalType() instanceof RowType)) {
            throw new ValidationException("Logical DataType must be a RowType");
        }

        RowType rowType = (RowType) dataType.getLogicalType();
        for (RowType.RowField field : rowType.getFields()) {
            // TODO: We can't convert VARBINARY(n) data type to
            //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
            // LegacyTypeInfoDataTypeConverter
            //  when n is smaller than Integer.MAX_VALUE
            if (unsupportedTypes().contains(field.getType().getTypeRoot())
                    || (field.getType() instanceof VarBinaryType
                            && Integer.MAX_VALUE
                                    != ((VarBinaryType) field.getType()).getLength())) {
                throw new ValidationException(
                        String.format(
                                "The %s dialect doesn't support type: %s.",
                                dialectName(), field.getType()));
            }

            if (field.getType() instanceof DecimalType) {
                int precision = ((DecimalType) field.getType()).getPrecision();
                if (precision > maxDecimalPrecision() || precision < minDecimalPrecision()) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the DECIMAL "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    field.getName(),
                                    minDecimalPrecision(),
                                    maxDecimalPrecision(),
                                    dialectName()));
                }
            }

            if (field.getType() instanceof TimestampType) {
                int precision = ((TimestampType) field.getType()).getPrecision();
                if (precision > maxTimestampPrecision() || precision < minTimestampPrecision()) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the TIMESTAMP "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    field.getName(),
                                    minTimestampPrecision(),
                                    maxTimestampPrecision(),
                                    dialectName()));
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JdbcDialect &&
                ((JdbcDialect) obj).dialectName().equals(dialectName());
    }

    @Override
    public int hashCode() {
        return dialectName().hashCode() >> 31;
    }

    public abstract int maxDecimalPrecision();

    public abstract int minDecimalPrecision();

    public abstract int maxTimestampPrecision();

    public abstract int minTimestampPrecision();

    /**
     * Defines the unsupported types for the dialect.
     *
     * @return a list of logical type roots.
     */
    public abstract List<LogicalTypeRoot> unsupportedTypes();
}
