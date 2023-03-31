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

package org.apache.flink.table.jdbc;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.apache.flink.table.jdbc.utils.DriverUtils.checkNotNull;

/** Column info for {@link ResultSetMetaData}, it is converted from {@link LogicalType}. */
public class ColumnInfo {
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();
    private static final int STRUCT_MAX = 100 * 1024 * 1024;

    private final int columnType;
    private final String columnTypeName;
    private final boolean nullable;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int columnDisplaySize;
    private final String columnName;

    private ColumnInfo(
            int columnType,
            String columnTypeName,
            boolean nullable,
            boolean signed,
            int precision,
            int scale,
            int columnDisplaySize,
            String columnName) {
        this.columnType = columnType;
        this.columnTypeName = columnTypeName;
        this.nullable = nullable;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.columnDisplaySize = columnDisplaySize;
        this.columnName = checkNotNull(columnName, "column name cannot be null");
    }

    public int getColumnType() {
        return columnType;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getColumnDisplaySize() {
        return columnDisplaySize;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String columnTypeName() {
        return columnTypeName;
    }

    /**
     * Build column info from given logical type.
     *
     * @param columnName the column name
     * @param type the logical type
     * @return the result column info
     */
    public static ColumnInfo fromLogicalType(String columnName, LogicalType type) {
        Builder builder =
                new Builder()
                        .columnName(columnName)
                        .nullable(type.isNullable())
                        .signed(type.is(LogicalTypeFamily.NUMERIC))
                        .columnTypeName(type.asSummaryString());
        if (type instanceof BooleanType) {
            // "true" or "false"
            builder.columnType(Types.BOOLEAN).columnDisplaySize(5);
        } else if (type instanceof TinyIntType) {
            builder.columnType(Types.TINYINT).precision(3).scale(0).columnDisplaySize(4);
        } else if (type instanceof SmallIntType) {
            builder.columnType(Types.SMALLINT).precision(5).scale(0).columnDisplaySize(6);
        } else if (type instanceof IntType) {
            builder.columnType(Types.INTEGER).precision(10).scale(0).columnDisplaySize(11);
        } else if (type instanceof BigIntType) {
            builder.columnType(Types.BIGINT).precision(19).scale(0).columnDisplaySize(20);
        } else if (type instanceof FloatType) {
            builder.columnType(Types.FLOAT).precision(9).scale(0).columnDisplaySize(16);
        } else if (type instanceof DoubleType) {
            builder.columnType(Types.DOUBLE).precision(17).scale(0).columnDisplaySize(24);
        } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            builder.columnType(Types.DECIMAL)
                    .columnDisplaySize(decimalType.getPrecision() + 2) // dot and sign
                    .precision(decimalType.getPrecision())
                    .scale(decimalType.getScale());
        } else if (type instanceof CharType) {
            CharType charType = (CharType) type;
            builder.columnType(Types.CHAR)
                    .scale(0)
                    .precision(charType.getLength())
                    .columnDisplaySize(charType.getLength());
        } else if (type instanceof VarCharType) {
            builder.columnType(Types.VARCHAR)
                    .scale(0)
                    .precision(VARBINARY_MAX)
                    .columnDisplaySize(VARBINARY_MAX);
        } else if (type instanceof BinaryType) {
            BinaryType binaryType = (BinaryType) type;
            builder.columnType(Types.BINARY)
                    .scale(0)
                    .precision(binaryType.getLength())
                    .columnDisplaySize(binaryType.getLength());
        } else if (type instanceof VarBinaryType) {
            builder.columnType(Types.VARBINARY)
                    .scale(0)
                    .precision(VARBINARY_MAX)
                    .columnDisplaySize(VARBINARY_MAX);
        } else if (type instanceof DateType) {
            builder.columnType(Types.DATE).scale(0).columnDisplaySize(DATE_MAX);
        } else if (type instanceof TimeType) {
            TimeType timeType = (TimeType) type;
            builder.columnType(Types.TIME)
                    .precision(timeType.getPrecision())
                    .scale(0)
                    .columnDisplaySize(TIME_MAX);
        } else if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            builder.columnType(Types.TIMESTAMP)
                    .precision(timestampType.getPrecision())
                    .scale(0)
                    .columnDisplaySize(TIMESTAMP_MAX);
        } else if (type instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
            builder.columnType(Types.TIMESTAMP)
                    .precision(localZonedTimestampType.getPrecision())
                    .scale(0)
                    .columnDisplaySize(TIMESTAMP_MAX);
        } else if (type instanceof ZonedTimestampType) {
            ZonedTimestampType zonedTimestampType = (ZonedTimestampType) type;
            builder.columnType(Types.TIMESTAMP_WITH_TIMEZONE)
                    .precision(zonedTimestampType.getPrecision())
                    .scale(0)
                    .columnDisplaySize(TIMESTAMP_WITH_TIME_ZONE_MAX);
        } else if (type instanceof ArrayType) {
            builder.columnType(Types.ARRAY)
                    .scale(0)
                    .precision(STRUCT_MAX)
                    .columnDisplaySize(STRUCT_MAX);
        } else if (type instanceof MapType) {
            // Use java object for map while there's no map in Types at the moment
            builder.columnType(Types.JAVA_OBJECT)
                    .scale(0)
                    .precision(STRUCT_MAX)
                    .columnDisplaySize(STRUCT_MAX);
        } else if (type instanceof RowType) {
            builder.columnType(Types.STRUCT)
                    .precision(STRUCT_MAX)
                    .scale(0)
                    .columnDisplaySize(STRUCT_MAX);
        } else {
            throw new RuntimeException(String.format("Not supported type[%s]", type));
        }
        return builder.build();
    }

    /** Builder to build {@link ColumnInfo} from {@link LogicalType}. */
    private static class Builder {
        private int columnType;
        private String columnTypeName;
        private boolean nullable;
        private boolean signed;
        private int precision;
        private int scale;
        private int columnDisplaySize;
        private String columnName;

        public Builder columnType(int columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder columnTypeName(String columnTypeName) {
            this.columnTypeName = columnTypeName;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder signed(boolean signed) {
            this.signed = signed;
            return this;
        }

        public Builder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder columnDisplaySize(int columnDisplaySize) {
            this.columnDisplaySize = columnDisplaySize;
            return this;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public ColumnInfo build() {
            return new ColumnInfo(
                    columnType,
                    columnTypeName,
                    nullable,
                    signed,
                    precision,
                    scale,
                    columnDisplaySize,
                    columnName);
        }
    }
}
