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

package org.apache.flink.orc;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Unit Tests for {@link OrcFileFormatFactory}. */
public class OrcFileSystemFilterTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyPredicate() {
        List<ResolvedExpression> args = new ArrayList<>();

        // equal
        FieldReferenceExpression fieldReferenceExpression =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression = new ValueLiteralExpression(10);
        args.add(fieldReferenceExpression);
        args.add(valueLiteralExpression);

        CallExpression equalExpression =
                new CallExpression(BuiltInFunctionDefinitions.EQUALS, args, DataTypes.BOOLEAN());
        OrcFilters.Predicate predicate1 = OrcFilters.toOrcPredicate(equalExpression);
        OrcFilters.Predicate predicate2 =
                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 10);
        assertTrue(predicate1.toString().equals(predicate2.toString()));

        // greater than
        CallExpression greaterExpression =
                new CallExpression(
                        BuiltInFunctionDefinitions.GREATER_THAN, args, DataTypes.BOOLEAN());
        OrcFilters.Predicate predicate3 = OrcFilters.toOrcPredicate(greaterExpression);
        OrcFilters.Predicate predicate4 =
                new OrcFilters.Not(
                        new OrcFilters.LessThanEquals("long1", PredicateLeaf.Type.LONG, 10));
        assertTrue(predicate3.toString().equals(predicate4.toString()));

        // less than
        CallExpression lessExpression =
                new CallExpression(BuiltInFunctionDefinitions.LESS_THAN, args, DataTypes.BOOLEAN());
        OrcFilters.Predicate predicate5 = OrcFilters.toOrcPredicate(lessExpression);
        OrcFilters.Predicate predicate6 =
                new OrcFilters.LessThan("long1", PredicateLeaf.Type.LONG, 10);
        assertTrue(predicate5.toString().equals(predicate6.toString()));
    }
}
