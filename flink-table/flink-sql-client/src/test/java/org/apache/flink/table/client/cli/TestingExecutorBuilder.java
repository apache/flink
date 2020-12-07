/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.TriFunctionWithException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Builder for {@link TestingExecutor}.
 */
class TestingExecutorBuilder {

	private String defaultCurrentCatalogName = "default_catalog";
	private String defaultCurrentDatabaseName = "default_database";
	private List<SupplierWithException<TypedResult<List<Tuple2<Boolean, Row>>>, SqlExecutionException>> resultChangesSupplier = Collections.emptyList();
	private List<SupplierWithException<TypedResult<Integer>, SqlExecutionException>> snapshotResultsSupplier = Collections.emptyList();
	private List<SupplierWithException<List<Row>, SqlExecutionException>> resultPagesSupplier = Collections.emptyList();
	private BiFunctionWithException<String, String, TableResult, SqlExecutionException> setExecuteSqlConsumer = (ignoredA, ignoredB) -> null;
	private TriFunctionWithException<String, String, String, Void, SqlExecutionException> setSessionPropertyFunction = (ignoredA, ignoredB, ignoredC) -> null;
	private FunctionWithException<String, Void, SqlExecutionException> resetSessionPropertiesFunction = (ignoredA) -> null;

	@SafeVarargs
	public final TestingExecutorBuilder setResultChangesSupplier(SupplierWithException<TypedResult<List<Tuple2<Boolean, Row>>>, SqlExecutionException> ... resultChangesSupplier) {
		this.resultChangesSupplier = Arrays.asList(resultChangesSupplier);
		return this;
	}

	@SafeVarargs
	public final TestingExecutorBuilder setSnapshotResultSupplier(SupplierWithException<TypedResult<Integer>, SqlExecutionException> ... snapshotResultsSupplier) {
		this.snapshotResultsSupplier = Arrays.asList(snapshotResultsSupplier);
		return this;
	}

	@SafeVarargs
	public final TestingExecutorBuilder setResultPageSupplier(SupplierWithException<List<Row>, SqlExecutionException> ... resultPageSupplier) {
		resultPagesSupplier = Arrays.asList(resultPageSupplier);
		return this;
	}

	public final TestingExecutorBuilder setExecuteSqlConsumer(
			BiFunctionWithException<String, String, TableResult, SqlExecutionException> setExecuteUpdateConsumer) {
		this.setExecuteSqlConsumer = setExecuteUpdateConsumer;
		return this;
	}

	public final TestingExecutorBuilder setSessionPropertiesFunction(TriFunctionWithException<String, String, String, Void, SqlExecutionException> setSessionPropertyFunction) {
		this.setSessionPropertyFunction = setSessionPropertyFunction;
		return this;
	}

	public final TestingExecutorBuilder resetSessionPropertiesFunction(FunctionWithException<String, Void, SqlExecutionException> resetSessionPropertiesFunction) {
		this.resetSessionPropertiesFunction = resetSessionPropertiesFunction;
		return this;
	}

	public TestingExecutor build() {
		return new TestingExecutor(
			resultChangesSupplier,
			snapshotResultsSupplier,
			resultPagesSupplier,
			setExecuteSqlConsumer,
			setSessionPropertyFunction,
			resetSessionPropertiesFunction);
	}
}
