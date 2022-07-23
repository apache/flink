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

package org.apache.flink.formats.parquet;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit Tests for {@link ParquetFilters}. */
public class ParquetFiltersTest {
    private final ParquetFilters parquetFilters = new ParquetFilters(true);

    @Test
    public void testApplyBinaryOperationPredicate() {
        List<ResolvedExpression> args = new ArrayList<>();

        // equal
        FieldReferenceExpression fieldReferenceExpression =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression = new ValueLiteralExpression(10);
        args.add(fieldReferenceExpression);
        args.add(valueLiteralExpression);

        CallExpression equalExpression =
                new CallExpression(BuiltInFunctionDefinitions.EQUALS, args, DataTypes.BOOLEAN());
        FilterPredicate expectPredicate = FilterApi.eq(FilterApi.longColumn("long1"), 10L);

        FilterPredicate actualPredicate = parquetFilters.toParquetPredicate(equalExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // not equal
        CallExpression notEqualExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.NOT_EQUALS, args, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.notEq(FilterApi.longColumn("long1"), 10L);
        actualPredicate = parquetFilters.toParquetPredicate(notEqualExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // greater than
        CallExpression greaterExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN, args, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.gt(FilterApi.longColumn("long1"), 10L);
        actualPredicate = parquetFilters.toParquetPredicate(greaterExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // greater than or equal
        CallExpression greaterEqualExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        args,
                        DataTypes.BOOLEAN());
        expectPredicate = FilterApi.gtEq(FilterApi.longColumn("long1"), 10L);
        actualPredicate = parquetFilters.toParquetPredicate(greaterEqualExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // less than
        CallExpression lessExpression =
                new CallExpression(BuiltInFunctionDefinitions.LESS_THAN, args, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.lt(FilterApi.longColumn("long1"), 10L);
        actualPredicate = parquetFilters.toParquetPredicate(lessExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // less than or equal
        CallExpression lessEqualExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, args, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.ltEq(FilterApi.longColumn("long1"), 10L);
        actualPredicate = parquetFilters.toParquetPredicate(lessEqualExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());
    }

    @Test
    public void testApplyBinaryLogicalPredicate() {
        // test and
        List<ResolvedExpression> args1 = new ArrayList<>();
        FieldReferenceExpression fieldReferenceExpression1 =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression1 = new ValueLiteralExpression(10);
        args1.add(fieldReferenceExpression1);
        args1.add(valueLiteralExpression1);

        CallExpression gtExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN, args1, DataTypes.BOOLEAN());

        List<ResolvedExpression> args2 = new ArrayList<>();
        FieldReferenceExpression fieldReferenceExpression2 =
                new FieldReferenceExpression("string1", DataTypes.STRING(), 0, 0);
        ValueLiteralExpression valueLiteralExpression2 = new ValueLiteralExpression("string1");
        args2.add(fieldReferenceExpression2);
        args2.add(valueLiteralExpression2);

        CallExpression equalExpression =
                new CallExpression(BuiltInFunctionDefinitions.EQUALS, args2, DataTypes.BOOLEAN());

        CallExpression callExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.AND,
                        Arrays.asList(gtExpression, equalExpression),
                        DataTypes.BOOLEAN());
        FilterPredicate expectPredicate =
                FilterApi.and(
                        FilterApi.gt(FilterApi.longColumn("long1"), 10L),
                        FilterApi.eq(
                                FilterApi.binaryColumn("string1"),
                                Binary.fromReusedByteArray("string1".getBytes())));
        FilterPredicate actualPredicate = parquetFilters.toParquetPredicate(callExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // test or
        callExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.OR,
                        Arrays.asList(gtExpression, equalExpression),
                        DataTypes.BOOLEAN());
        actualPredicate = parquetFilters.toParquetPredicate(callExpression);
        expectPredicate =
                FilterApi.or(
                        FilterApi.gt(FilterApi.longColumn("long1"), 10L),
                        FilterApi.eq(
                                FilterApi.binaryColumn("string1"),
                                Binary.fromReusedByteArray("string1".getBytes())));
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());
    }

    @Test
    public void testApplyUnaryPredicate() {
        // test not
        List<ResolvedExpression> args1 = new ArrayList<>();
        FieldReferenceExpression fieldReferenceExpression =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression = new ValueLiteralExpression(10);
        args1.add(fieldReferenceExpression);
        args1.add(valueLiteralExpression);
        CallExpression gtExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN, args1, DataTypes.BOOLEAN());

        CallExpression notExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.NOT,
                        Collections.singletonList(gtExpression),
                        DataTypes.BOOLEAN());
        FilterPredicate expectPredicate =
                FilterApi.not(FilterApi.gt(FilterApi.longColumn("long1"), 10L));
        FilterPredicate actualPredicate = parquetFilters.toParquetPredicate(notExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // test is null
        args1 = new ArrayList<>();
        fieldReferenceExpression = new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        args1.add(fieldReferenceExpression);
        CallExpression nullExpression =
                new CallExpression(BuiltInFunctionDefinitions.IS_NULL, args1, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.eq(FilterApi.longColumn("long1"), null);
        actualPredicate = parquetFilters.toParquetPredicate(nullExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());

        // test is not null
        CallExpression notNullExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.IS_NOT_NULL, args1, DataTypes.BOOLEAN());
        expectPredicate = FilterApi.not(FilterApi.eq(FilterApi.longColumn("long1"), null));
        actualPredicate = parquetFilters.toParquetPredicate(notNullExpression);
        assertNotNull(actualPredicate);
        assertEquals(expectPredicate.toString(), actualPredicate.toString());
    }

    @Test
    public void testIsFilterFieldsIn() {
        List<ResolvedExpression> args1 = new ArrayList<>();
        FieldReferenceExpression fieldReferenceExpression1 =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression1 = new ValueLiteralExpression(10);
        args1.add(fieldReferenceExpression1);
        args1.add(valueLiteralExpression1);

        CallExpression gtExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN, args1, DataTypes.BOOLEAN());

        List<ResolvedExpression> args2 = new ArrayList<>();
        FieldReferenceExpression fieldReferenceExpression2 =
                new FieldReferenceExpression("string1", DataTypes.STRING(), 0, 0);
        ValueLiteralExpression valueLiteralExpression2 = new ValueLiteralExpression("string1");
        args2.add(fieldReferenceExpression2);
        args2.add(valueLiteralExpression2);

        CallExpression equalExpression =
                new CallExpression(BuiltInFunctionDefinitions.EQUALS, args2, DataTypes.BOOLEAN());

        CallExpression callExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.AND,
                        Arrays.asList(gtExpression, equalExpression),
                        DataTypes.BOOLEAN());

        FilterPredicate predicate = parquetFilters.toParquetPredicate(callExpression);

        assertTrue(
                ParquetFilters.isFilterFieldsIn(
                        predicate, Arrays.asList("long1", "string1", "string2")));

        assertFalse(ParquetFilters.isFilterFieldsIn(predicate, Arrays.asList("long1", "string2")));
    }
}
