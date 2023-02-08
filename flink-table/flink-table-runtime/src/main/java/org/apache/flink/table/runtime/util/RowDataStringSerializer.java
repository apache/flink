package org.apache.flink.table.runtime.util;


import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.Row;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.Serializable;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class RowDataStringSerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowType type;

    public RowDataStringSerializer(InternalTypeInfo<RowData> type) {
        if (type != null) {
            this.type = type.toRowType();
        } else {
            this.type = null;
        }
    }

    public RowDataStringSerializer(RowType type) {
        this.type = type;
    }

    public String asString(RowData row) {
        // XXX(sergei): we're not always dilligent in checking that all callers pass in
        // type information into the operator that does logging, so put in a safeguard
        // in this class to prevent null pointer exceptions
        if (type == null) {
            return "[API error: RowDataStringSerializer got a `null` type, cannot decode row]";
        }
        LogicalType[] fieldTypes =
        type.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        int rowArity = type.getFieldCount();
        String rowString = "[" + row.getRowKind().toString() + "] {";
        for (int i = 0; i < rowArity; i++) {
            String value = "";
            if (row.isNullAt(i)) {
                value = "<NULL>";
            } else {
                DecimalType decimalType;
                TimestampType timestampType;
                LocalZonedTimestampType localZonedTimestampType;

                switch (fieldTypes[i].getTypeRoot()) {
                        case NULL:
                            value = "<NULL>";
                            break;

                        case BOOLEAN:
                            value = row.getBoolean(i) ? "True" : "False";
                            break;

                        case INTEGER:
                        case INTERVAL_YEAR_MONTH:
                            value = Integer.toString(row.getInt(i));
                            break;

                        case BIGINT:
                        case INTERVAL_DAY_TIME:
                            value = Long.toString(row.getLong(i));
                            break;

                        case CHAR:
                        case VARCHAR:
                            value = "'" + row.getString(i).toString() + "'";
                            break;

                        case FLOAT:
                            value = Float.toString(row.getFloat(i));
                            break;

                        case DOUBLE:
                            value = Double.toString(row.getDouble(i));
                            break;
                        
                        case TIME_WITHOUT_TIME_ZONE:
                            value = timeWithoutTimeZoneToString(row.getInt(i));
                            break;
                        
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            timestampType = (TimestampType) fieldTypes[i];
                            value = timestampWithoutTimeZoneToString(row.getTimestamp(i, timestampType.getPrecision()));
                            break;

                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            localZonedTimestampType = (LocalZonedTimestampType) fieldTypes[i];
                            value = timestampWithLocalTimeZoneToString(row.getTimestamp(i, localZonedTimestampType.getPrecision()));
                            break;

                        case DECIMAL:
                            decimalType = (DecimalType) fieldTypes[i];
                            value = row.getDecimal(i, decimalType.getPrecision(), decimalType.getScale()).toString();
                            break;

                        case DATE:
                            value = dateToString(row.getInt(i));
                            break;

                        case ARRAY:
                            value = "[ARRAY TYPE]";
                            break;

                        case MAP:
                            value = "[MAP TYPE]";
                            break;

                        case MULTISET:
                            value = "[MULTISET]";
                            break;

                        default:
                            value = "[Unprocessed type]";
                            break;
                }
            }
            String field = fieldNames[i] + "=" + value;
            rowString = rowString + field + (i == rowArity - 1 ? "" : ", ");
        }
        return rowString + "}";
    }

    private String dateToString(int days) {
        LocalDate date = LocalDate.ofEpochDay(days);
        return ISO_LOCAL_DATE.format(date);
    }

    private String timeWithoutTimeZoneToString(int millis) {
        LocalTime time = LocalTime.ofSecondOfDay(millis / 1000L);
        return TimeFormats.SQL_TIME_FORMAT.format(time);
    }

    private String timestampWithoutTimeZoneToString(TimestampData timestamp) {
        return TimeFormats.SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
    }

    private String timestampWithLocalTimeZoneToString(TimestampData timestampWithLocalZone) {
        return TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
            timestampWithLocalZone
                    .toInstant()
                    .atOffset(ZoneOffset.UTC));
    }

    static final class TimeFormats {
        /** Formatter for RFC 3339-compliant string representation of a time value. */
        public static final DateTimeFormatter RFC3339_TIME_FORMAT =
                new DateTimeFormatterBuilder()
                        .appendPattern("HH:mm:ss")
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                        .appendPattern("'Z'")
                        .toFormatter();
    
        /**
         * Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC
         * timezone).
         */
        public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral('T')
                        .append(RFC3339_TIME_FORMAT)
                        .toFormatter();
    
        /** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
        public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT =
                DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
        /** Formatter for ISO8601 string representation of a timestamp value (with UTC timezone). */
        public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral('T')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .appendPattern("'Z'")
                        .toFormatter();
    
        /** Formatter for SQL string representation of a time value. */
        public static final DateTimeFormatter SQL_TIME_FORMAT =
                new DateTimeFormatterBuilder()
                        .appendPattern("HH:mm:ss")
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                        .toFormatter();
    
        /** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
        public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(SQL_TIME_FORMAT)
                        .toFormatter();
    
        /** Formatter for SQL string representation of a timestamp value (with UTC timezone). */
        public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(SQL_TIME_FORMAT)
                        .appendPattern("'Z'")
                        .toFormatter();
    
        private TimeFormats() {}
    }
}
