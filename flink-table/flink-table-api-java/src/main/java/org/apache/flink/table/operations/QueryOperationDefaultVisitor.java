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
 * A utility {@link QueryOperationVisitor} that calls
 * {@link QueryOperationDefaultVisitor#defaultMethod(QueryOperation)}
 * by default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class QueryOperationDefaultVisitor<T> implements QueryOperationVisitor<T> {

	@Override
	public T visitProject(ProjectQueryOperation projection) {
		return defaultMethod(projection);
	}

	@Override
	public T visitAggregate(AggregateQueryOperation aggregation) {
		return defaultMethod(aggregation);
	}

	@Override
	public T visitWindowAggregate(WindowAggregateQueryOperation windowAggregate) {
		return defaultMethod(windowAggregate);
	}

	@Override
	public T visitJoin(JoinQueryOperation join) {
		return defaultMethod(join);
	}

	@Override
	public T visitSetOperation(SetQueryOperation setOperation) {
		return defaultMethod(setOperation);
	}

	@Override
	public T visitFilter(FilterQueryOperation filter) {
		return defaultMethod(filter);
	}

	@Override
	public T visitDistinct(DistinctQueryOperation distinct) {
		return defaultMethod(distinct);
	}

	@Override
	public T visitSort(SortQueryOperation sort) {
		return defaultMethod(sort);
	}

	@Override
	public T visitCalculatedTable(CalculatedQueryOperation calculatedTable) {
		return defaultMethod(calculatedTable);
	}

	@Override
	public T visitCatalogTable(CatalogQueryOperation catalogTable) {
		return defaultMethod(catalogTable);
	}

	@Override
	public <U> T visitTableSourceTable(TableSourceQueryOperation<U> tableSourceTable) {
		return defaultMethod(tableSourceTable);
	}

	@Override
	public T visitOther(QueryOperation other) {
		return defaultMethod(other);
	}

	public abstract T defaultMethod(QueryOperation other);
}
