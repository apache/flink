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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import java.util.List;

/**
 * An utility class that provides abilities to parse sql statements.
 */
public class ParserUtils {

	private static final TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

	static {
		tableEnv.executeSql("create table MyTable (a int, b bigint, c varchar(32)) " +
				"with ('connector' = 'filesystem', 'path' = '/non')");
		tableEnv.executeSql("create table MyOtherTable (a int, b bigint) " +
				"with ('connector' = 'filesystem', 'path' = '/non')");
		tableEnv.executeSql("create table MySink (a int, c varchar(32)) with ('connector' = 'COLLECTION' )");
	}

	public static List<Operation> parse(String statement) throws SqlExecutionException {
		try {
			return ((TableEnvironmentInternal) tableEnv).getParser().parse(statement);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}
}
