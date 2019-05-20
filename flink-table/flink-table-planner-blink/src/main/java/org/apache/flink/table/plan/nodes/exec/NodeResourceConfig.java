/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.exec;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Deal with resource config for {@link org.apache.flink.table.plan.nodes.exec.ExecNode}.
 */
public class NodeResourceConfig {

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MIN =
			key("sql.resource.infer.operator.parallelism.min")
					.defaultValue(1)
					.withDescription("Sets min parallelism for operators.");

	/**
	 * Gets the config max num of operator parallelism.
	 *
	 * @param tableConf Configuration.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMaxParallelism(Configuration tableConf) {
		return tableConf.getInteger(TableConfigOptions.SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MAX);
	}

	/**
	 * Gets the config min num of operator parallelism.
	 *
	 * @param tableConf Configuration.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMinParallelism(Configuration tableConf) {
		return tableConf.getInteger(SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MIN);
	}

	/**
	 * Gets the config row count that one partition processes.
	 *
	 * @param tableConf Configuration.
	 * @return the config row count that one partition processes.
	 */
	public static long getRowCountPerPartition(Configuration tableConf) {
		return tableConf.getLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION);
	}

	/**
	 * Calculates operator parallelism based on rowcount of the operator.
	 *
	 * @param rowCount rowCount of the operator
	 * @param tableConf Configuration.
	 * @return the result of operator parallelism.
	 */
	public static int calOperatorParallelism(double rowCount, Configuration tableConf) {
		int maxParallelism = getOperatorMaxParallelism(tableConf);
		int minParallelism = getOperatorMinParallelism(tableConf);
		int resultParallelism = (int) (rowCount / getRowCountPerPartition(tableConf));
		return Math.max(Math.min(resultParallelism, maxParallelism), minParallelism);
	}

}
