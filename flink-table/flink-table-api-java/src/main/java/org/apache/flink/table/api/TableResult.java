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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Optional;

/**
 * A TableResult is the representation of the statement execution result.
 */
@PublicEvolving
public interface TableResult {

	/**
	 * For DML and DQL statement, return the {@link JobClient}
	 * which associates the submitted Flink job.
	 * For other statements (e.g.  DDL, DCL) return empty.
	 */
	Optional<JobClient> getJobClient();

	/**
	 * Get the schema of result.
	 *
	 * <p>The schema of DDL, SHOW, EXPLAIN:
	 * <pre>
	 * +------------+-----------+-----------+
	 * | field name | field type | comments |
	 * +------------+------------+----------+
	 * | result     | STRING     |          |
	 * +------------+------------+----------+
	 * </pre>
	 *
	 * <p>The schema of DESCRIBE:
	 * <pre>
	 * +-----------------+------------+-----------------------------------------------------------------------------+
	 * |   field name    | field type |                              comments                                       |
	 * +-----------------+------------+-----------------------------------------------------------------------------+
	 * | name            | STRING     | field name                                                                  |
	 * | type            | STRING     | field type expressed as a String                                            |
	 * | null            | BOOLEAN    | field nullability: true if it's nullable, else false                        |
	 * | key             | BOOLEAN    | key constraint: 'PRI' for primary keys, 'UNQ' for unique keys, else null    |
	 * | computed column | STRING     | computed column: string expression if a field is computed column, else null |
	 * | watermark       | STRING     | watermark: string expression if a field is a watermark, else null           |
	 * +-----------------+------------+-----------------------------------------------------------------------------+
	 * </pre>
	 *
	 * <p>The schema of INSERT: (one column per one sink)
	 * <pre>
	 * +----------------------------+------------+-----------------------------------+
	 * | field name                 | field type |               comments            |
	 * +----------------------------+------------+-----------------------------------+
	 * | (name of the insert table) | BIGINT     | field name is the sink table name |
	 * +----------------------------+------------+-----------------------------------+
	 * </pre>
	 *
	 * <p>The schema of SELECT is the selected field names and types.
	 */
	TableSchema getTableSchema();

	/**
	 * Return the {@link ResultKind} which represents the result type.
	 */
	ResultKind getResultKind();

	/**
	 * Get the result contents as a closeable row iterator.
	 *
	 * <p><strong>NOTE:</strong>
	 * <ul>
	 *     <li>
	 *         For SELECT operation, the job will not be finished unless all result data has been collected.
	 *         So we should actively close the job to avoid resource leak through CloseableIterator#close method.
	 *         Calling CloseableIterator#close method will cancel the job and release related resources.
	 *     </li>
	 *     <li>
	 *          For INSERT operation, Flink does not support getting the affected row count now.
	 *          So the affected row count is always -1 (unknown) for every sink, and the constant row
	 *          will be will be returned after the insert job is submitted.
	 *          Do nothing when calling CloseableIterator#close method (which will not cancel the job
	 *          because the returned iterator does not bound to the job now).
	 *          We can cancel the job through {@link #getJobClient()} if needed.
	 *     </li>
	 *     <li>
	 *         For other operations, no flink job will be submitted. So all result is bounded.
	 *         Do nothing when calling CloseableIterator#close method.
	 *     </li>
	 * </ul>
	 *
	 * <p>Recommended code to call CloseableIterator#close method looks like:
	 * <pre>{@code
	 *  TableResult result = tEnv.execute("select ...");
	 *  // using try-with-resources statement
	 *  try (CloseableIterator<Row> it = result.collect()) {
	 *      it... // collect same data
	 *  }
	 * }</pre>
	 */
	CloseableIterator<Row> collect();

	/**
	 * Print the result contents as tableau form to client console.
	 */
	void print();
}
