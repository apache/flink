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

package org.apache.flink.connector.jdbc.internal.converter;

import oracle.jdbc.internal.OracleClob;

import oracle.sql.TIMESTAMPLTZ;

import oracle.sql.TIMESTAMPTZ;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.sql.CLOB;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * Oracle.
 */
public class OracleRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public OracleRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        // TODO: please consider all Oracle Data Types like oracle.sql.OracleBlob,
        // oracle.sql.TIMESTAMPTZ and oracle.sql.TIMESTAMPLTZ etc in deserialization.
        // We can refer io.debezium.connector.oracle.OracleValueConverters here.
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> val instanceof NUMBER ? ((NUMBER) val).booleanValue() : val;
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val instanceof NUMBER ? ((NUMBER) val).intValue() : val;
            case FLOAT:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).floatValue()
                                : val instanceof BINARY_FLOAT
                                        ? ((BINARY_FLOAT) val).floatValue()
                                        : val instanceof BigDecimal
                                                ? ((BigDecimal) val).floatValue()
                                                : val;
            case DOUBLE:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).doubleValue()
                                : val instanceof BINARY_DOUBLE
                                        ? ((BINARY_DOUBLE) val).doubleValue()
                                        : val instanceof BigDecimal
                                                ? ((BigDecimal) val).doubleValue()
                                                : val;
            case TINYINT:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).byteValue()
                                : val instanceof BigDecimal ? ((BigDecimal) val).byteValue() : val;
            case SMALLINT:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).shortValue()
                                : val instanceof BigDecimal ? ((BigDecimal) val).shortValue() : val;
            case INTEGER:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).intValue()
                                : val instanceof BigDecimal ? ((BigDecimal) val).intValue() : val;
            case BIGINT:
                return val ->
                        val instanceof NUMBER
                                ? ((NUMBER) val).longValue()
                                : val instanceof BigDecimal ? ((BigDecimal) val).longValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        val instanceof DATE
                                ? ((DATE) val).dateValue().toLocalDate().toEpochDay()
                                : (int) (((Date) val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof DATE
                                ? (int)
                                        (((DATE) val).timeValue().toLocalTime().toNanoOfDay()
                                                / 1_000_000L)
                                : (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case CHAR:
            case VARCHAR:
                return val ->
                        (val instanceof CHAR)
                                ? StringData.fromString(((CHAR) val).getString())
                                : (val instanceof OracleClob)
                                    ? StringData.fromString(((OracleClob) val).stringValue())
                                    : StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
            case RAW:
                return val -> (val instanceof RAW) ? ((RAW) val).getBytes() : val;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof TIMESTAMP
                                ? TimestampData.fromTimestamp(((TIMESTAMP) val).timestampValue())
                                : TimestampData.fromTimestamp((Timestamp) val);
            case TIMESTAMP_WITH_TIME_ZONE:
                return val -> {
                    if (val instanceof TIMESTAMPTZ) {
                        final TIMESTAMPTZ ts = (TIMESTAMPTZ) val;
                        final ZonedDateTime zdt = ZonedDateTime
                                .ofInstant(ts.timestampValue().toInstant(), ts.getTimeZone().toZoneId());
                        return TimestampData.fromLocalDateTime(zdt.toLocalDateTime());
                    } else {
                        return TimestampData.fromTimestamp((Timestamp) val);
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    if (val instanceof TIMESTAMPLTZ) {
                        final TIMESTAMPLTZ ts = (TIMESTAMPLTZ) val;
                        final ZonedDateTime zdt = ZonedDateTime
                                .ofInstant(ts.timestampValue().toInstant(), ZoneId.systemDefault())
                                .withZoneSameInstant(ZoneOffset.UTC);
                        return TimestampData.fromLocalDateTime(zdt.toLocalDateTime());
                    } else {
                        return TimestampData.fromTimestamp((Timestamp) val);
                    }
                };
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            default:
                return super.createInternalConverter(type);
        }
    }

    @Override
    public String converterName() {
        return "Oracle";
    }
}
