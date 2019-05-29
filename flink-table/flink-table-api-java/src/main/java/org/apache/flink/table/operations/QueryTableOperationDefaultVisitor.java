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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

/**
 * A utility {@link QueryTableOperationVisitor} that calls
 * {@link QueryTableOperationDefaultVisitor#defaultMethod(QueryTableOperation)}
 * by default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class QueryTableOperationDefaultVisitor<T> implements QueryTableOperationVisitor<T> {

	@Override
	public T visitProject(ProjectQueryTableOperation projection) {
		return defaultMethod(projection);
	}

	@Override
	public T visitAggregate(AggregateQueryTableOperation aggregation) {
		return defaultMethod(aggregation);
	}

	@Override
	public T visitWindowAggregate(WindowAggregateQueryTableOperation windowAggregate) {
		return defaultMethod(windowAggregate);
	}

	@Override
	public T visitJoin(JoinQueryTableOperation join) {
		return defaultMethod(join);
	}

	@Override
	public T visitSetOperation(SetQueryTableOperation setOperation) {
		return defaultMethod(setOperation);
	}

	@Override
	public T visitFilter(FilterQueryTableOperation filter) {
		return defaultMethod(filter);
	}

	@Override
	public T visitDistinct(DistinctQueryTableOperation distinct) {
		return defaultMethod(distinct);
	}

	@Override
	public T visitSort(SortQueryTableOperation sort) {
		return defaultMethod(sort);
	}

	@Override
	public T visitCalculatedTable(CalculatedQueryTableOperation calculatedTable) {
		return defaultMethod(calculatedTable);
	}

	@Override
	public T visitCatalogTable(CatalogQueryTableOperation catalogTable) {
		return defaultMethod(catalogTable);
	}

	@Override
	public T visitCatalogSink(CatalogSinkTableOperation catalogSink) {
		return defaultMethod(catalogSink);
	}

	@Override
	public <U> T visitInlineSink(InlineSinkTableOperation<U> inlineSink) {
		return defaultMethod(inlineSink);
	}

	@Override
	public <U> T visitOutputConversion(OutputConversionTableOperation<U> outputConversion) {
		return defaultMethod(outputConversion);
	}

	@Override
	public T visitOther(QueryTableOperation other) {
		return defaultMethod(other);
	}

	public abstract T defaultMethod(QueryTableOperation other);
}
