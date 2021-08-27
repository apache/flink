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
 * Class that implements visitor pattern. It allows type safe logic on top of tree of {@link
 * QueryOperation}s.
 */
@Internal
public interface QueryOperationVisitor<T> {

    T visit(ProjectQueryOperation projection);

    T visit(AggregateQueryOperation aggregation);

    T visit(WindowAggregateQueryOperation windowAggregate);

    T visit(JoinQueryOperation join);

    T visit(SetQueryOperation setOperation);

    T visit(FilterQueryOperation filter);

    T visit(DistinctQueryOperation distinct);

    T visit(SortQueryOperation sort);

    T visit(CalculatedQueryOperation calculatedTable);

    T visit(CatalogQueryOperation catalogTable);

    T visit(ValuesQueryOperation values);

    <U> T visit(TableSourceQueryOperation<U> tableSourceTable);

    T visit(QueryOperation other);
}
