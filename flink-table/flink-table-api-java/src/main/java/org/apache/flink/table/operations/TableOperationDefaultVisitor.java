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
 * A utility {@link TableOperationVisitor} that calls
 * {@link TableOperationDefaultVisitor#defaultMethod(TableOperation)}
 * by default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class TableOperationDefaultVisitor<T> implements TableOperationVisitor<T> {

	@Override
	public T visitProject(ProjectTableOperation projection) {
		return defaultMethod(projection);
	}

	@Override
	public T visitAggregate(AggregateTableOperation aggregation) {
		return defaultMethod(aggregation);
	}

	@Override
	public T visitWindowAggregate(WindowAggregateTableOperation windowAggregate) {
		return defaultMethod(windowAggregate);
	}

	@Override
	public T visitJoin(JoinTableOperation join) {
		return defaultMethod(join);
	}

	@Override
	public T visitSetOperation(SetTableOperation setOperation) {
		return defaultMethod(setOperation);
	}

	@Override
	public T visitFilter(FilterTableOperation filter) {
		return defaultMethod(filter);
	}

	@Override
	public T visitDistinct(DistinctTableOperation distinct) {
		return defaultMethod(distinct);
	}

	@Override
	public T visitSort(SortTableOperation sort) {
		return defaultMethod(sort);
	}

	@Override
	public T visitCalculatedTable(CalculatedTableOperation calculatedTable) {
		return defaultMethod(calculatedTable);
	}

	@Override
	public T visitCatalogTable(CatalogTableOperation catalogTable) {
		return defaultMethod(catalogTable);
	}

	@Override
	public T visitOther(TableOperation other) {
		return defaultMethod(other);
	}

	public abstract T defaultMethod(TableOperation other);
}
