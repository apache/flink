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
	 * <p>The schema of DDL, USE, SHOW, EXPLAIN:
	 * <pre>
	 * +-------------+-------------+----------+
	 * | column name | column type | comments |
	 * +-------------+-------------+----------+
	 * | result      | STRING      |          |
	 * +-------------+-------------+----------+
	 * </pre>
	 *
	 * <p>The schema of DESCRIBE:
	 * <pre>
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * | column name      | column type |                              comments                                       |
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * | name             | STRING      | field name                                                                  |
	 * | type             | STRING      | field type expressed as a String                                            |
	 * | null             | BOOLEAN     | field nullability: true if a field is nullable, else false                  |
	 * | key              | BOOLEAN     | key constraint: 'PRI' for primary keys, 'UNQ' for unique keys, else null    |
	 * | computed column  | STRING      | computed column: string expression if a field is computed column, else null |
	 * | watermark        | STRING      | watermark: string expression if a field is watermark, else null             |
	 * +------------------+-------------+-----------------------------------------------------------------------------+
	 * </pre>
	 *
	 * <p>The schema of INSERT: (one column per one sink)
	 * <pre>
	 * +----------------------------+-------------+-----------------------+
	 * | column name                | column type | comments              |
	 * +----------------------------+-------------+-----------------------+
	 * | (name of the insert table) | BIGINT      | the insert table name |
	 * +----------------------------+-------------+-----------------------+
	 * </pre>
	 *
	 * <p>The schema of SELECT is the selected field names and types.
	 */
	TableSchema getTableSchema();

	/**
	 * Return the {@link ResultKind} which represents the result type.
	 *
	 * <p>For DDL operation and USE operation, the result kind is always {@link ResultKind#SUCCESS}.
	 * For other operations, the result kind is always {@link ResultKind#SUCCESS_WITH_CONTENT}.
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
	 *         For DML operation, Flink does not support getting the real affected row count now.
	 *         So the affected row count is always -1 (unknown) for every sink, and them will be
	 *         returned after the job is submitted.
	 *         Calling CloseableIterator#close method does not bind to the job.
	 *         Therefore the `CloseableIterator#close` will not cancel the job as in the case of SELECT.
	 *         If you need to cancel the job, you can use the {@link #getJobClient()}.
	 *     </li>
	 *     <li>
	 *         For other operations, no flink job will be submitted ({@link #getJobClient()} is always empty),
	 *         and the result is bounded. Do nothing when calling CloseableIterator#close method.
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
	 *
	 * <p><strong>NOTE:</strong> please make sure the result data to print should be small.
	 * Because all data will be collected to local first, and then print them to console.
	 */
	void print();
}
