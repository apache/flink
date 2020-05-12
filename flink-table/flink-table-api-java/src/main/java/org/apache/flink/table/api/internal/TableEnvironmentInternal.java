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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

import java.util.List;

/**
 * An internal interface of {@link TableEnvironment}
 * that defines extended methods used for {@link TableImpl}.
 *
 * <p>Once old planner is removed, this class also can be removed.
 * By then, these methods can be moved into TableEnvironmentImpl.
 */
@Internal
interface TableEnvironmentInternal extends TableEnvironment {

	/**
	 * Return a {@link Parser} that provides methods for parsing a SQL string.
	 *
	 * @return initialized {@link Parser}.
	 */
	Parser getParser();

	/**
	 * Returns a {@link CatalogManager} that deals with all catalog objects.
	 */
	CatalogManager getCatalogManager();

	/**
	 * Execute the given modify operations and return the execution result.
	 *
	 * @param operations The operations to be executed.
	 * @return the affected row counts (-1 means unknown).
	 */
	TableResult executeInternal(List<ModifyOperation> operations);

	/**
	 * Execute the given query operation and return the execution result.
	 *
	 * @param operation The QueryOperation to be executed.
	 * @return the content of the QueryOperation.
	 */
	TableResult executeInternal(QueryOperation operation);

	/**
	 * Returns the AST of this table and the execution plan to compute
	 * the result of this table.
	 *
	 * @param operations The operations to be explained.
	 * @param extraDetails The extra explain details which the explain result should include,
	 *   e.g. estimated cost, changelog mode for streaming
	 * @return AST and the execution plan.
	 */
	String explainInternal(List<Operation> operations, ExplainDetail... extraDetails);

}
