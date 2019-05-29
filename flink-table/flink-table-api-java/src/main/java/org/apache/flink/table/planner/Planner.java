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

package org.apache.flink.table.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.operations.ModifyTableOperation;
import org.apache.flink.table.operations.QueryTableOperation;
import org.apache.flink.table.operations.TableOperation;

import java.util.List;

/**
 * This interface serves two purposes:
 * <ul>
 * <li>SQL parser - transforms a SQL string into a Table API specific tree of
 * {@link TableOperation}s</li>
 * <li>relational planner - provides a way to plan, optimize and transform tree of
 * {@link ModifyTableOperation} into a runnable form ({@link StreamTransformation})</li>
 * </ul>.
 *
 * <p>The Planner is execution agnostic. It is up to the
 * {@link org.apache.flink.table.api.TableEnvironment} to ensure that if any of the
 * {@link QueryTableOperation} pull any runtime configuration, all those configurations are
 * equivalent. Example: If some of the {@link QueryTableOperation}s scan DataStreams, all
 * those DataStreams must come from the same StreamExecutionEnvironment, because the result
 * of {@link Planner#translate(List, QueryConfig)} will strip any execution configuration from
 * the DataStream information.
 *
 * <p>All Tables referenced in either {@link Planner#parse(String)} or
 * {@link Planner#translate(List, QueryConfig)} should be previously registered in a
 * {@link org.apache.flink.table.catalog.CatalogManager}, which will be provided during
 * instantiation of the {@link Planner}.
 */
@Internal
public interface Planner {

	/**
	 * Entry point for parsing sql queries expressed as a String.
	 *
	 * <p><b>Note:</b>If the created {@link TableOperation} is a {@link QueryTableOperation}
	 * it must be in a form that will be understood by the
	 * {@link Planner#translate(List, QueryConfig)} method.
	 *
	 * <p>The produced Operation tree should already be validated.
	 *
	 * @param stmt the sql statement to evaluate
	 * @return parsed query as a tree of relational {@link TableOperation}s
	 */
	TableOperation parse(String stmt);

	/**
	 * Converts a relational tree of {@link QueryTableOperation}s into a set of runnable
	 * {@link StreamTransformation}s.
	 *
	 * <p>This method accepts a list of {@link QueryTableOperation}s to allow reusing common
	 * subtrees of multiple relational queries.
	 *
	 * @param tableOperations list of relational operations to plan, optimize and convert in a
	 * single run.
	 * @param queryConfig allows configuring runtime behavior of the resulting query
	 * @return list of corresponding {@link StreamTransformation}s.
	 */
	List<StreamTransformation<?>> translate(List<ModifyTableOperation> tableOperations, QueryConfig queryConfig);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan
	 * to compute the result of the given collection of {@link QueryTableOperation}s.
	 *
	 * @param queryTableOperations The collection of relational queries for which the AST
	 * and execution plan will be returned.
	 */
	String explain(List<QueryTableOperation> queryTableOperations, QueryConfig queryConfig);

	/**
	 * Returns completion hints for the given statement at the given cursor position.
	 * The completion happens case insensitively.
	 *
	 * @param statement Partial or slightly incorrect SQL statement
	 * @param position cursor position
	 * @return completion hints that fit at the current cursor position
	 */
	String[] getCompletionHints(String statement, int position);
}
