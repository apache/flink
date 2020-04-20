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
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Builder for {@link TestingExecutor}.
 */
class TestingExecutorBuilder {

	private List<SupplierWithException<TypedResult<List<Tuple2<Boolean, Row>>>, SqlExecutionException>> resultChangesSupplier = Collections.emptyList();
	private List<SupplierWithException<TypedResult<Integer>, SqlExecutionException>> snapshotResultsSupplier = Collections.emptyList();
	private List<SupplierWithException<List<Row>, SqlExecutionException>> resultPagesSupplier = Collections.emptyList();
	private BiConsumerWithException<String, String, SqlExecutionException> setUseCatalogConsumer = (ignoredA, ignoredB) -> {};
	private BiConsumerWithException<String, String, SqlExecutionException> setUseDatabaseConsumer = (ignoredA, ignoredB) -> {};

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

	public final TestingExecutorBuilder setUseCatalogConsumer(BiConsumerWithException<String, String, SqlExecutionException> useCatalogConsumer) {
		this.setUseCatalogConsumer = useCatalogConsumer;
		return this;
	}

	public final TestingExecutorBuilder setUseDatabaseConsumer(BiConsumerWithException<String, String, SqlExecutionException> useDatabaseConsumer) {
		this.setUseDatabaseConsumer = useDatabaseConsumer;
		return this;
	}

	public TestingExecutor build() {
		return new TestingExecutor(
			resultChangesSupplier,
			snapshotResultsSupplier,
			resultPagesSupplier,
			setUseCatalogConsumer,
			setUseDatabaseConsumer);
	}
}
