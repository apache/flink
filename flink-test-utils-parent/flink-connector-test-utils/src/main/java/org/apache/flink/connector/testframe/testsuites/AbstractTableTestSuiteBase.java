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

package org.apache.flink.connector.testframe.testsuites;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.testframe.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connector.testframe.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
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
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static org.apache.flink.types.RowKind.INSERT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Abstract class for table test suites. */
@ExtendWith({
    ConnectorTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Experimental
public abstract class AbstractTableTestSuiteBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTableTestSuiteBase.class);

    private DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");
    private DateTimeFormatter timestampFormat =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private DateTimeFormatter flinkTimestampFormat =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public abstract List<DataType> supportTypes();

    /** Generate the select sql. */
    protected String getSelectSql(String tableName) {
        String query = "SELECT * FROM " + tableName;
        LOG.info("Query result: " + query);
        return query;
    }

    /** Generate the create table sql. */
    protected String getCreateTableSql(
            String tableName, Map<String, String> tableOptions, List<DataType> dataTypes) {
        String createTableSql =
                String.format(
                        "CREATE TABLE `%s` (\n" + " %s\n" + ") WITH (\n" + "%s\n" + ")",
                        tableName, getSchemaInSql(dataTypes), getTableOptions(tableOptions));
        LOG.info("Create table: " + createTableSql);
        return createTableSql;
    }

    private String getTableOptions(Map<String, String> tableOptions) {
        return tableOptions.entrySet().stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(joining(",\n"));
    }

    /** Generate the schema part in the create table sql. */
    private String getSchemaInSql(List<DataType> dataTypes) {
        return IntStream.range(0, dataTypes.size())
                .mapToObj(
                        index -> {
                            DataType dataType = dataTypes.get(index);
                            return String.format(
                                    "`%s` %s",
                                    getFieldName(index, dataType), getTypeForSql(dataType));
                        })
                .collect(joining(",\n"));
    }

    /** Get the table schema . */
    protected DataType getTableSchema(List<DataType> dataTypes) {
        return ResolvedSchema.of(
                        IntStream.range(0, dataTypes.size())
                                .mapToObj(
                                        index -> {
                                            DataType dataType = dataTypes.get(index);
                                            return Column.physical(
                                                    getFieldName(index, dataType), dataType);
                                        })
                                .collect(Collectors.toList()))
                .toPhysicalRowDataType();
    }

    public static RowData convertToRowData(Row row, DataType dataType) {
        List<DataType> dataTypes = DataType.getFieldDataTypes(dataType);
        assertThat(dataTypes.size()).isEqualTo(row.getArity());

        Object[] fieldVals = new Object[row.getArity()];
        for (int i = 0; i < fieldVals.length; i++) {
            fieldVals[i] = convertToInternalType(row.getField(i), dataTypes.get(i));
        }
        return GenericRowData.ofKind(INSERT, fieldVals);
    }

    private String getTypeForSql(DataType type) {
        return type.getLogicalType().toString();
    }

    private String getFieldName(int index, DataType type) {
        return type.getLogicalType().getClass().getSimpleName().toLowerCase() + index;
    }

    protected Tuple2<String, List<RowData>> initialValues(
            String tableName, long seed, int uppedBound, int lowerBound, List<DataType> dataTypes) {
        Random random = new Random(seed);
        int recordNum = random.nextInt(uppedBound - lowerBound) + lowerBound;
        List<List<Tuple2<String, Object>>> testData = new LinkedList<>();
        for (int i = 0; i < recordNum; i++) {
            testData.add(generateSinkData(dataTypes));
        }
        String initialValueSql =
                String.format(
                        "INSERT INTO %s VALUES %s",
                        tableName,
                        testData.stream()
                                .map(l -> l.stream().map(o -> o.f0).collect(joining(",")))
                                .map(s -> "(" + s + ")")
                                .collect(joining(",")));
        LOG.info("Insert data: " + initialValueSql);
        return Tuple2.of(initialValueSql, getExpectedResult(testData));
    }

    private List<RowData> getExpectedResult(List<List<Tuple2<String, Object>>> testData) {
        List<RowData> expected = new LinkedList<>();
        for (List<Tuple2<String, Object>> row : testData) {
            Object[] fields = new Object[row.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = row.get(i).f1;
            }
            expected.add(GenericRowData.ofKind(INSERT, fields));
        }
        return expected;
    }

    protected RowData generateTestRowData(int spiltId, List<DataType> supportTypes) {
        Object[] fieldVals = new Object[supportTypes.size()];
        for (int i = 0; i < fieldVals.length; i++) {
            fieldVals[i] = generateTestDataForField(spiltId, supportTypes.get(i));
        }
        return GenericRowData.ofKind(INSERT, fieldVals);
    }

    private List<Tuple2<String, Object>> generateSinkData(List<DataType> supportTypes) {
        // f0 ---- sql value, f1 ---- expected value
        List<Tuple2<String, Object>> oneRow = new LinkedList<>();
        for (DataType dataType : supportTypes) {
            oneRow.add(getOneFieldData(dataType));
        }
        return oneRow;
    }

    private String generateRandomString(int length) {
        Random random = new Random();
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length + 1; ++i) {
            sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
        }
        return sb.toString();
    }

    /** Get the internal data structure for different {@link DataType}. */
    public static Object convertToInternalType(Object val, DataType type) {
        LogicalType logicalType = type.getLogicalType();
        // String
        if (logicalType instanceof CharType || logicalType instanceof VarCharType) {
            return StringData.fromString((String) val);
        }
        // boolean
        if (logicalType instanceof BooleanType) {
            return val;
        }
        // byte[]
        if (logicalType instanceof BinaryType || logicalType instanceof VarBinaryType) {
            return val;
        }
        // BigDecimal
        if (logicalType instanceof DecimalType) {
            BigDecimal bd = new BigDecimal((double) val);
            return DecimalData.fromBigDecimal(bd, bd.precision(), 9);
        }
        // byte
        if (logicalType instanceof TinyIntType) {
            return val;
        }
        // short
        if (logicalType instanceof SmallIntType) {
            return val;
        }
        // int
        if (logicalType instanceof IntType) {
            return val;
        }
        // long
        if (logicalType instanceof BigIntType) {
            return val;
        }
        // float
        if (logicalType instanceof FloatType) {
            return val;
        }
        // double
        if (logicalType instanceof DoubleType) {
            return val;
        }
        // LocalDate
        if (logicalType instanceof DateType) {
            return (int) ((LocalDate) val).toEpochDay();
        }
        // LocalTime
        if (logicalType instanceof TimeType) {
            return (int) (((LocalTime) val).toNanoOfDay() / 1000 / 1000) / 1000 * 1000;
        }
        // LocalDateTime | OffsetDateTime
        if (logicalType instanceof TimestampType || logicalType instanceof ZonedTimestampType) {
            return TimestampData.fromLocalDateTime((LocalDateTime) val);
        }
        // Instant
        if (logicalType instanceof LocalZonedTimestampType) {
            return TimestampData.fromLocalDateTime(
                    ((Instant) val).atZone(ZoneOffset.UTC).toLocalDateTime());
        }
        throw new IllegalArgumentException("Cannot convert val[%s] for type[%s]");
    }

    /** Return flink internal data. */
    private Object generateTestDataForField(int splitId, DataType type) {
        if (type instanceof AtomicDataType) {
            Random random = new Random();
            LogicalType logicalType = type.getLogicalType();
            // String
            if (logicalType instanceof CharType) {
                return StringData.fromString(
                        String.format(
                                "%d-%s",
                                splitId,
                                generateRandomString(((CharType) logicalType).getLength())));
            }
            if (logicalType instanceof VarCharType) {
                int length = ((VarCharType) logicalType).getLength();
                return StringData.fromString(
                        generateRandomString(random.nextInt(length > 10 ? 10 : length)));
            }
            // boolean
            if (logicalType instanceof BooleanType) {
                return random.nextInt(10) % 2 == 0;
            }
            // byte[]
            if (logicalType instanceof BinaryType || logicalType instanceof VarBinaryType) {
                return generateRandomString(random.nextInt(3)).getBytes();
            }
            // BigDecimal
            if (logicalType instanceof DecimalType) {
                BigDecimal bd = new BigDecimal(random.nextDouble());
                return DecimalData.fromBigDecimal(bd, bd.precision(), 9);
            }
            // byte
            if (logicalType instanceof TinyIntType) {
                return (byte) random.nextInt(100);
            }
            // short
            if (logicalType instanceof SmallIntType) {
                return (short) random.nextInt(20);
            }
            // int
            if (logicalType instanceof IntType) {
                return random.nextInt();
            }
            // long
            if (logicalType instanceof BigIntType) {
                return random.nextLong();
            }
            // float
            if (logicalType instanceof FloatType) {
                return (float) Math.round(random.nextFloat() * 10000) / 10000;
            }
            // double
            if (logicalType instanceof DoubleType) {
                return (double) Math.round(random.nextDouble() * 1000000) / 1000000;
            }
            // LocalDate
            if (logicalType instanceof DateType) {
                return (int) LocalDate.now().toEpochDay();
            }
            // LocalTime
            if (logicalType instanceof TimeType) {
                ((TimeType) logicalType).getPrecision();
                return (int) (LocalTime.now().toNanoOfDay() / 1000 / 1000) / 1000 * 1000;
            }
            // LocalDateTime | OffsetDateTime | Instant
            if (logicalType instanceof TimestampType
                    || logicalType instanceof ZonedTimestampType
                    || logicalType instanceof LocalZonedTimestampType) {
                return TimestampData.fromLocalDateTime(
                        getLocalDateTime(System.currentTimeMillis()));
            }
        }
        throw new IllegalArgumentException("Not support non AtomicDataType.");
    }

    /** Return flink internal data and sql field. */
    private Tuple2<String, Object> getOneFieldData(DataType type) {
        if (type instanceof AtomicDataType) {
            Random random = new Random();
            LogicalType logicalType = type.getLogicalType();
            // String
            if (logicalType instanceof CharType) {
                String randomString = generateRandomString(((CharType) logicalType).getLength());
                return Tuple2.of(
                        "'" + randomString + "'", convertToInternalType(randomString, type));
            }
            if (logicalType instanceof VarCharType) {
                int length = ((VarCharType) logicalType).getLength();
                String randomString =
                        generateRandomString(random.nextInt(length > 10 ? 10 : length));
                return Tuple2.of(
                        "'" + randomString + "'", convertToInternalType(randomString, type));
            }
            // boolean
            if (logicalType instanceof BooleanType) {
                boolean booleanVal = random.nextInt(10) % 2 == 0;
                return Tuple2.of(
                        String.valueOf(booleanVal), convertToInternalType(booleanVal, type));
            }
            // byte[]
            if (logicalType instanceof BinaryType || logicalType instanceof VarBinaryType) {
                String randomString = generateRandomString(random.nextInt(3));
                return Tuple2.of(
                        "'" + randomString + "'",
                        convertToInternalType(randomString.getBytes(), type));
            }
            // BigDecimal
            if (logicalType instanceof DecimalType) {
                double target = random.nextDouble();
                BigDecimal decimal = new BigDecimal(target);
                return Tuple2.of(decimal.toString(), convertToInternalType(target, type));
            }
            // byte
            if (logicalType instanceof TinyIntType) {
                byte byteVal = (byte) random.nextInt(100);
                return Tuple2.of(
                        String.format("CAST ('%d' AS TINYINT)", byteVal),
                        convertToInternalType(byteVal, type));
            }
            // short
            if (logicalType instanceof SmallIntType) {
                short shortVal = (short) random.nextInt(20);
                return Tuple2.of(
                        String.format("CAST ('%d' AS SMALLINT)", shortVal),
                        convertToInternalType(shortVal, type));
            }
            // int
            if (logicalType instanceof IntType) {
                int intVal = random.nextInt();
                return Tuple2.of(String.valueOf(intVal), convertToInternalType(intVal, type));
            }
            // long
            if (logicalType instanceof BigIntType) {
                long longVal = random.nextLong();
                return Tuple2.of(String.valueOf(longVal), convertToInternalType(longVal, type));
            }
            // float
            if (logicalType instanceof FloatType) {
                float floatVal = (float) Math.round(random.nextFloat() * 10000) / 10000;
                return Tuple2.of(
                        String.format("CAST ('%s' AS FLOAT)", String.valueOf(floatVal)),
                        convertToInternalType(floatVal, type));
            }
            // double
            if (logicalType instanceof DoubleType) {
                double doubleVal = (double) Math.round(random.nextDouble() * 1000000) / 1000000;
                return Tuple2.of(String.valueOf(doubleVal), convertToInternalType(doubleVal, type));
            }
            // LocalDate
            if (logicalType instanceof DateType) {
                LocalDate target = getLocalDate(System.currentTimeMillis());
                return Tuple2.of(
                        String.format("DATE '%s'", target.format(dateFormat)),
                        convertToInternalType(target, type));
            }
            // LocalTime
            if (logicalType instanceof TimeType) {
                LocalTime target = getLocalTime(System.currentTimeMillis());
                return Tuple2.of(
                        String.format("TIME '%s'", target.format(timeFormat)),
                        convertToInternalType(target, type));
            }
            // LocalDateTime
            if (logicalType instanceof TimestampType) {
                LocalDateTime localDateTime = getLocalDateTime(System.currentTimeMillis());
                return Tuple2.of(
                        String.format("TIMESTAMP '%s'", localDateTime.format(timestampFormat)),
                        convertToInternalType(localDateTime, type));
            }
            // OffsetDateTime
            if (logicalType instanceof ZonedTimestampType) {
                LocalDateTime localDateTime = getLocalDateTime(System.currentTimeMillis());
                return Tuple2.of(
                        String.format("TIMESTAMP '%s'", localDateTime.format(timestampFormat)),
                        convertToInternalType(localDateTime, type));
            }
            // Instant
            if (logicalType instanceof LocalZonedTimestampType) {
                long ms = System.currentTimeMillis();
                LocalDateTime localDateTime = getLocalDateTime(ms);
                return Tuple2.of(
                        String.format("TO_TIMESTAMP_LTZ(%d, 3)", ms),
                        convertToInternalType(Instant.ofEpochMilli(ms), type));
            }
        }
        throw new IllegalArgumentException("Not support non AtomicDataType.");
    }

    private LocalDate getLocalDate(long ms) {
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalDate();
    }

    private LocalTime getLocalTime(long ms) {
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalTime();
    }

    private LocalDateTime getLocalDateTime(long ms) {
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalDateTime();
    }
}
