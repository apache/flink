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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Tests for HiveTableUtil.
 */
public class HiveTableUtilTest {

	private static final HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

	@Test
	public void testMakePartitionFilter() {
		List<String> partColNames = Arrays.asList("p1", "p2", "p3");
		ResolvedExpression p1Ref = new FieldReferenceExpression("p1", DataTypes.INT(), 0, 2);
		ResolvedExpression p2Ref = new FieldReferenceExpression("p2", DataTypes.STRING(), 0, 3);
		ResolvedExpression p3Ref = new FieldReferenceExpression("p3", DataTypes.DOUBLE(), 0, 4);
		ResolvedExpression p1Exp = new CallExpression(BuiltInFunctionDefinitions.EQUALS,
				Arrays.asList(p1Ref, new ValueLiteralExpression(1, DataTypes.INT())), DataTypes.BOOLEAN());
		ResolvedExpression p2Exp = new CallExpression(BuiltInFunctionDefinitions.EQUALS,
				Arrays.asList(p2Ref, new ValueLiteralExpression("a", DataTypes.STRING())), DataTypes.BOOLEAN());
		ResolvedExpression p3Exp = new CallExpression(BuiltInFunctionDefinitions.EQUALS,
				Arrays.asList(p3Ref, new ValueLiteralExpression(1.1, DataTypes.DOUBLE())), DataTypes.BOOLEAN());
		Optional<String> filter = HiveTableUtil.makePartitionFilter(2, partColNames, Arrays.asList(p1Exp), hiveShim);
		assertEquals("(p1 = 1)", filter.orElse(null));

		filter = HiveTableUtil.makePartitionFilter(2, partColNames, Arrays.asList(p1Exp, p3Exp), hiveShim);
		assertEquals("(p1 = 1) and (p3 = 1.1)", filter.orElse(null));

		filter = HiveTableUtil.makePartitionFilter(2, partColNames,
				Arrays.asList(p2Exp,
						new CallExpression(BuiltInFunctionDefinitions.OR, Arrays.asList(p1Exp, p3Exp), DataTypes.BOOLEAN())),
				hiveShim);
		assertEquals("(p2 = 'a') and ((p1 = 1) or (p3 = 1.1))", filter.orElse(null));
	}
}
