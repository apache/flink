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
	 */
	TableSchema getTableSchema();

	/**
	 * Return the {@link ResultKind} which represents the result type.
	 */
	ResultKind getResultKind();

	/**
	 * Get the result contents as a closeable row iterator.
	 *
	 * <p><strong>NOTE:</strong>If this result corresponds to a flink job,
	 * the job will not be finished unless all result data has been collected.
	 * So we should actively close the job to avoid resource leak.
	 *
	 * <p>There are two approaches to close a job:
	 * 1. close the job through JobClient, for example:
	 <pre>{@code
	 *  TableResult result = tEnv.execute("select ...");
	 *  CloseableIterator<Row> it = result.collect();
	 *  it... // collect same data
	 *  result.getJobClient().get().cancel();
	 * }</pre>
	 *
	 * 2. close the job through CloseableIterator
	 * (calling CloseableIterator#close method will trigger JobClient#cancel method),
	 * for example:
	 <pre>{@code
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
