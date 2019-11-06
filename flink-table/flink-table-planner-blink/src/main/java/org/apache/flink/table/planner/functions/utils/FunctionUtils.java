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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.Optional;

/**
 * Utils for sql functions.
 */
public class FunctionUtils {

	public static FunctionIdentifier toFunctionIdentifier(String[] names, CatalogManager catalogManager) {
		return names.length == 1 ?
				FunctionIdentifier.of(names[0]) :
				FunctionIdentifier.of(
						catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(names)));
	}

	public static FunctionIdentifier toFunctionIdentifier(CallExpression call, UserDefinedFunction function) {
		return call.getFunctionIdentifier()
				.orElse(FunctionIdentifier.of(function.functionIdentifier()));
	}

	public static SqlIdentifier toSqlIdentifier(FunctionIdentifier fi) {
		Optional<ObjectIdentifier> objectIdentifier = fi.getIdentifier();
		String[] names = objectIdentifier
				.map(id -> new String[] {id.getCatalogName(), id.getDatabaseName(), id.getObjectName()})
				.orElseGet(() -> new String[]{fi.getSimpleName().get()});
		return new SqlIdentifier(Arrays.asList(names), SqlParserPos.ZERO);
	}
}
