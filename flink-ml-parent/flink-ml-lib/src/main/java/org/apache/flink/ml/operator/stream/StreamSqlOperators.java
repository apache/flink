/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.operator.stream;

import org.apache.flink.ml.common.MLEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.operator.AlgoOperator;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Apply sql operators(select, where, filter, union etc.) on {@link StreamOperator}s.
 *
 * <p>It is package private to allow access from {@link StreamOperator}.
 */
class StreamSqlOperators {

	/**
	 * Get the {@link MLEnvironment} of the <code>AlgoOperator</code>.
	 */
	private static MLEnvironment getMLEnv(AlgoOperator algoOp) {
		return MLEnvironmentFactory.get(algoOp.getMLEnvironmentId());
	}

	/**
	 * Register the output table of a StreamOperator to the {@link StreamTableEnvironment}
	 * with a temporary table name.
	 *
	 * @param streamOp The StreamOperator who's output table is being registered.
	 * @return The temporary table name.
	 */
	private static String registerTempTable(StreamOperator streamOp) {
		StreamTableEnvironment tEnv = getMLEnv(streamOp).getStreamTableEnvironment();
		String tmpTableName = TableUtil.getTempTableName();
		tEnv.registerTable(tmpTableName, streamOp.getOutput());
		return tmpTableName;
	}

	/**
	 * Evaluate the "select" query on the StreamOperator.
	 *
	 * @param fields The query fields.
	 * @return The evaluation result as a StreamOperator.
	 */
	public static StreamOperator select(StreamOperator streamOp, String fields) {
		String tmpTableName = registerTempTable(streamOp);
		return getMLEnv(streamOp).streamSQL(String.format("SELECT %s FROM %s", fields, tmpTableName));
	}

	/**
	 * Rename the fields of a StreamOperator.
	 *
	 * @param fields Comma separated field names.
	 * @return The StreamOperator after renamed.
	 */
	public static StreamOperator as(StreamOperator streamOp, String fields) {
		return StreamOperator.fromTable(streamOp.getOutput().as(fields));
	}

	/**
	 * Apply the "where" operation on the StreamOperator.
	 *
	 * @param predicate The filter conditions.
	 * @return The filter result.
	 */
	public static StreamOperator where(StreamOperator streamOp, String predicate) {
		String tmpTableName = registerTempTable(streamOp);
		return getMLEnv(streamOp).streamSQL(String.format("SELECT * FROM %s WHERE %s", tmpTableName, predicate));
	}

	/**
	 * Apply the "filter" operation on the StreamOperator.
	 *
	 * @param predicate The filter conditions.
	 * @return The filter result.
	 */
	public static StreamOperator filter(StreamOperator streamOp, String predicate) {
		return where(streamOp, predicate);
	}

	/**
	 * Union with another <code>StreamOperator</code>, the duplicated records are kept.
	 *
	 * @param leftOp  BatchOperator on the left hand side.
	 * @param rightOp BatchOperator on the right hand side.
	 * @return The resulted <code>StreamOperator</code> of the "unionAll" operation.
	 */
	public static StreamOperator unionAll(StreamOperator leftOp, StreamOperator rightOp) {
		return StreamOperator.fromTable(leftOp.getOutput().unionAll(rightOp.getOutput()));
	}
}
