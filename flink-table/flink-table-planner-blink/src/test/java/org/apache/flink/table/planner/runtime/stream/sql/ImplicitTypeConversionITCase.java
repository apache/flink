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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.scala.DataStreamConversions;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.planner.codegen.CodeGenException;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.planner.runtime.utils.TestingAppendRowDataSink;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.LegacyRowResource;

import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.junit.Assert.assertEquals;

/** test implicit type conversion between different types. */
public class ImplicitTypeConversionITCase extends StreamingTestBase {

    @Rule public LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    private List<String> testSingleTableSqlQueryWithOutputType(
            String sqlQuery, InternalTypeInfo<RowData> outputType) {
        GenericRowData rowData = new GenericRowData(14);
        rowData.setField(0, (byte) 1);
        rowData.setField(1, (short) 1);
        rowData.setField(2, 1);
        rowData.setField(3, (long) 1);
        rowData.setField(4, DecimalDataUtils.castFrom(1, 1, 0));
        rowData.setField(5, (float) 1);
        rowData.setField(6, (double) 1);
        int date = (int) LocalDate.parse("2001-01-01").toEpochDay();
        rowData.setField(7, date);
        int time = (int) (LocalTime.parse("00:00:00").toNanoOfDay() / 1000000L);
        rowData.setField(8, time);
        TimestampData timestamp =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2001-01-01T00:00:00"));
        rowData.setField(9, timestamp);
        rowData.setField(10, StringData.fromString("1"));
        rowData.setField(11, StringData.fromString("2001-01-01"));
        rowData.setField(12, StringData.fromString("00:00:00"));
        rowData.setField(13, StringData.fromString("2001-01-01 00:00:00"));
        List data = Arrays.asList(rowData);

        TypeInformation<RowData> tpe =
                InternalTypeInfo.ofFields(
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new DecimalType(1, 0),
                        new FloatType(),
                        new DoubleType(),
                        new DateType(),
                        new TimeType(),
                        new TimestampType(),
                        new VarCharType(),
                        new VarCharType(),
                        new VarCharType(),
                        new VarCharType());

        DataStream ds = env().fromCollection(JavaScalaConversionUtil.toScala(data), tpe);
        DataStreamConversions conversions = new DataStreamConversions(ds, tpe);

        List<UnresolvedReferenceExpression> fields =
                Arrays.asList(
                        unresolvedRef("field_tinyint"),
                        unresolvedRef("field_smallint"),
                        unresolvedRef("field_int"),
                        unresolvedRef("field_bigint"),
                        unresolvedRef("field_decimal"),
                        unresolvedRef("field_float"),
                        unresolvedRef("field_double"),
                        unresolvedRef("field_date"),
                        unresolvedRef("field_time"),
                        unresolvedRef("field_timestamp"),
                        unresolvedRef("field_varchar_equals_numeric"),
                        unresolvedRef("field_varchar_equals_date"),
                        unresolvedRef("field_varchar_equals_time"),
                        unresolvedRef("field_varchar_equals_timestamp"));

        Table table = conversions.toTable(tEnv(), JavaScalaConversionUtil.toScala(fields));
        tEnv().registerTable("TestTable", table);

        Table resultTable = tEnv().sqlQuery(sqlQuery);
        DataStream result = tEnv().toAppendStream(resultTable, outputType);
        TestingAppendRowDataSink sink = new TestingAppendRowDataSink(outputType);
        result.addSink(sink);
        env().execute();

        return new ArrayList<>(JavaScalaConversionUtil.toJava(sink.getAppendResults()));
    }

    @Test
    public void testNumericConversionInFilter() {
        String sqlQuery =
                "SELECT field_tinyint, field_smallint, field_int, field_bigint, "
                        + "field_decimal, field_float, field_double "
                        + "FROM TestTable WHERE "
                        + "field_tinyint = field_smallint AND "
                        + "field_tinyint = field_int AND "
                        + "field_tinyint = field_bigint AND "
                        + "field_tinyint = field_decimal AND "
                        + "field_tinyint = field_float AND "
                        + "field_tinyint = field_double AND "
                        + "field_smallint = field_int AND "
                        + "field_smallint = field_bigint AND "
                        + "field_smallint = field_decimal AND "
                        + "field_smallint = field_float AND "
                        + "field_smallint = field_double AND "
                        + "field_int = field_bigint AND "
                        + "field_int = field_decimal AND "
                        + "field_int = field_float AND "
                        + "field_int = field_double AND "
                        + "field_bigint = field_decimal AND "
                        + "field_bigint = field_float AND "
                        + "field_bigint = field_double AND "
                        + "field_decimal = field_float AND "
                        + "field_decimal = field_double AND "
                        + "field_float = field_double";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new DecimalType(),
                        new FloatType(),
                        new DoubleType());

        List<String> expected = Arrays.asList("+I(1,1,1,1,1,1.0,1.0)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    @Test
    public void testDateAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_date, field_varchar_equals_date FROM TestTable "
                        + "WHERE field_date = field_varchar_equals_date";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new DateType(), new VarCharType());

        List<String> expected = Arrays.asList("+I(11323,2001-01-01)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    @Test
    public void testTimeAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_time, field_varchar_equals_time FROM TestTable "
                        + "WHERE field_time = field_varchar_equals_time";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new TimeType(), new VarCharType());

        List<String> expected = Arrays.asList("+I(0,00:00:00)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    @Test
    public void testTimestampAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_timestamp, field_varchar_equals_timestamp FROM TestTable "
                        + "WHERE field_timestamp = field_varchar_equals_timestamp";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new TimestampType(), new VarCharType());

        List<String> expected = Arrays.asList("+I(2001-01-01T00:00,2001-01-01 00:00:00)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    private String getFilterAndProjectionExceptionMessage(List<String> types) {
        return String.format(
                "implicit type conversion between " + "%s and %s" + " is not supported now",
                types.get(0), types.get(1));
    }

    private void testSingleTableInvalidImplicitConversionTypes(
            String sqlQuery, InternalTypeInfo<RowData> outputType, List<String> types) {
        expectedException().expect(CodeGenException.class);
        expectedException().expectMessage(getFilterAndProjectionExceptionMessage(types));
        testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
    }

    @Test
    public void testInvalidTinyIntAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_tinyint, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_tinyint = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new TinyIntType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("TINYINT", "VARCHAR"));
    }

    @Test
    public void testInvalidSmallIntAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_smallint, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_smallint = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new SmallIntType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("SMALLINT", "VARCHAR"));
    }

    @Test
    public void testInvalidIntAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_int, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_int = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("INTEGER", "VARCHAR"));
    }

    @Test
    public void testInvalidBigIntAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_bigint, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_bigint = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new BigIntType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("BIGINT", "VARCHAR"));
    }

    @Test
    public void testInvalidDecimalAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_decimal, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_decimal = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new DecimalType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("DECIMAL", "VARCHAR"));
    }

    @Test
    public void testInvalidFloatAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_float, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_float = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new FloatType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("FLOAT", "VARCHAR"));
    }

    @Test
    public void testInvalidDoubleAndVarCharConversionInFilter() {
        String sqlQuery =
                "SELECT field_double, field_varchar_equals_numeric FROM TestTable "
                        + "WHERE field_double = field_varchar_equals_numeric";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new DoubleType(), new VarCharType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("DOUBLE", "VARCHAR"));
    }

    @Test
    public void testFloatAndDoubleConversionInProjection() {
        String sqlQuery =
                "SELECT field_float, field_double, field_float = field_double FROM TestTable";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new FloatType(), new DoubleType(), new BooleanType());

        List<String> expected = Arrays.asList("+I(1.0,1.0,true)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    @Test
    public void testDateAndVarCharConversionInProjection() {
        String sqlQuery =
                "SELECT field_date, field_varchar_equals_date, "
                        + "field_date = field_varchar_equals_date FROM TestTable";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new DateType(), new VarCharType(), new BooleanType());

        List<String> expected = Arrays.asList("+I(11323,2001-01-01,true)");

        List<String> actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType);
        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    @Test
    public void testInvalidDecimalAndVarCharConversionInProjection() {
        String sqlQuery =
                "SELECT field_decimal, field_varchar_equals_numeric, "
                        + "field_decimal = field_varchar_equals_numeric FROM TestTable";
        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new DecimalType(), new VarCharType(), new BooleanType());

        testSingleTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("DECIMAL", "VARCHAR"));
    }

    private void registerTableA() {
        GenericRowData rowDataA = new GenericRowData(6);
        rowDataA.setField(0, 1);
        rowDataA.setField(1, 1);
        rowDataA.setField(2, 1);
        int date = (int) LocalDate.parse("2001-01-01").toEpochDay();
        rowDataA.setField(3, date);
        int time = (int) (LocalTime.parse("00:00:00").toNanoOfDay() / 1000000L);
        rowDataA.setField(4, time);
        TimestampData timestamp =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2001-01-01T00:00:00"));
        rowDataA.setField(5, timestamp);
        List<RowData> dataA = Arrays.asList(rowDataA);

        TypeInformation<RowData> tpeA =
                InternalTypeInfo.ofFields(
                        new IntType(),
                        new IntType(),
                        new IntType(),
                        new DateType(),
                        new TimeType(),
                        new TimestampType());

        DataStream dsA = env().fromCollection(JavaScalaConversionUtil.toScala(dataA), tpeA);
        DataStreamConversions conversions = new DataStreamConversions(dsA, tpeA);

        List<UnresolvedReferenceExpression> fields =
                Arrays.asList(
                        unresolvedRef("a1"),
                        unresolvedRef("a2"),
                        unresolvedRef("a3"),
                        unresolvedRef("a4"),
                        unresolvedRef("a5"),
                        unresolvedRef("a6"));

        Table tableA = conversions.toTable(tEnv(), JavaScalaConversionUtil.toScala(fields));

        tEnv().registerTable("A", tableA);
    }

    private void registerTableB() {
        GenericRowData rowDataB = new GenericRowData(6);
        rowDataB.setField(0, 1);
        rowDataB.setField(1, (long) 1);
        rowDataB.setField(2, StringData.fromString("1"));
        rowDataB.setField(3, StringData.fromString("2001-01-01"));
        rowDataB.setField(4, StringData.fromString("00:00:00"));
        rowDataB.setField(5, StringData.fromString("2001-01-01 00:00:00"));
        List<RowData> dataB = Arrays.asList(rowDataB);

        TypeInformation<RowData> tpeB =
                InternalTypeInfo.ofFields(
                        new IntType(),
                        new BigIntType(),
                        new VarCharType(),
                        new VarCharType(),
                        new VarCharType(),
                        new VarCharType());

        DataStream dsB = env().fromCollection(JavaScalaConversionUtil.toScala(dataB), tpeB);
        DataStreamConversions conversions = new DataStreamConversions(dsB, tpeB);

        List<UnresolvedReferenceExpression> fields =
                Arrays.asList(
                        unresolvedRef("b1"),
                        unresolvedRef("b2"),
                        unresolvedRef("b3"),
                        unresolvedRef("b4"),
                        unresolvedRef("b5"),
                        unresolvedRef("b6"));

        Table tableB = conversions.toTable(tEnv(), JavaScalaConversionUtil.toScala(fields));

        tEnv().registerTable("B", tableB);
    }

    private void testTwoTableJoinSqlQuery(String sqlQuery, InternalTypeInfo<RowData> outputType) {
        registerTableA();
        registerTableB();

        Table resultTable = tEnv().sqlQuery(sqlQuery);
        DataStream result = tEnv().toAppendStream(resultTable, outputType);
        TestingAppendRowDataSink sink = new TestingAppendRowDataSink(outputType);
        result.addSink(sink);
        env().execute();

        List<String> expected = Arrays.asList("+I(1,1)");

        List<String> actualResult =
                new ArrayList<>(JavaScalaConversionUtil.toJava(sink.getAppendResults()));

        Collections.sort(expected);
        Collections.sort(actualResult);
        assertEquals(expected, actualResult);
    }

    private void testTwoTableInvalidImplicitConversionTypes(
            String sqlQuery, InternalTypeInfo<RowData> outputType, List<String> types) {
        expectedException().expect(TableException.class);
        expectedException().expectMessage(getJoinOnExceptionMessage(types));
        testTwoTableJoinSqlQuery(sqlQuery, outputType);
    }

    private String getJoinOnExceptionMessage(List<String> types) {
        return String.format(
                "implicit type conversion between "
                        + "%s and %s"
                        + " is not supported on join's condition now",
                types.get(0), types.get(1));
    }

    @Test
    public void testIntAndBigIntConversionInJoinOn() {
        String sqlQuery = "SELECT a1, b1 from A join B on a2 = b2";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new IntType());

        testTwoTableJoinSqlQuery(sqlQuery, outputType);
    }

    @Test
    public void testInvalidIntAndVarCharConversionInJoinOn() {
        String sqlQuery = "SELECT a1, b1 from A join B on a3 = b3";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new IntType());

        testTwoTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("INTEGER", "VARCHAR(1)"));
    }

    @Test
    public void testInvalidDateAndVarCharConversionInJoinOn() {
        String sqlQuery = "SELECT a1, b1 from A join B on a4 = b4";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new IntType());

        testTwoTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("DATE", "VARCHAR(1)"));
    }

    @Test
    public void testInvalidTimeAndVarCharConversionInJoinOn() {
        String sqlQuery = "SELECT a1, b1 from A join B on a5 = b5";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new IntType());

        testTwoTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("TIME(0)", "VARCHAR(1)"));
    }

    @Test
    public void testInvalidTimestampAndVarCharConversionInJoinOn() {
        String sqlQuery = "SELECT a1, b1 from A join B on a6 = b6";

        InternalTypeInfo<RowData> outputType =
                InternalTypeInfo.ofFields(new IntType(), new IntType());

        testTwoTableInvalidImplicitConversionTypes(
                sqlQuery, outputType, Arrays.asList("TIMESTAMP(6)", "VARCHAR(1)"));
    }
}
