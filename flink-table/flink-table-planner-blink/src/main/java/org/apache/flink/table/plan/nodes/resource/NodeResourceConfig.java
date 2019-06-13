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

package org.apache.flink.table.plan.nodes.resource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;

/**
 * Deal with resource config for {@link org.apache.flink.table.plan.nodes.exec.ExecNode}.
 */
public class NodeResourceConfig {

	/**
	 * How many Bytes per MB.
	 */
	public static final long SIZE_IN_MB =  1024L * 1024;

	/**
	 * Gets the config parallelism for source.
	 * @param tableConf Configuration.
	 * @return the config parallelism for source.
	 */
	public static int getSourceParallelism(Configuration tableConf, int envParallelism) {
		int parallelism = tableConf.getInteger(
				TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM);
		if (parallelism <= 0) {
			parallelism = getOperatorDefaultParallelism(tableConf, envParallelism);
		}
		return parallelism;
	}

	/**
	 * Gets the config parallelism for sink. If it is not set, return -1.
	 * @param tableConf Configuration.
	 * @return the config parallelism for sink.
	 */
	public static int getSinkParallelism(Configuration tableConf) {
		return tableConf.getInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM);
	}

	/**
	 * Gets the config max num of source parallelism.
	 * @param tableConf Configuration.
	 * @return the config max num of source parallelism.
	 */
	public static int getSourceMaxParallelism(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX);
	}

	/**
	 * Gets the config row count that one partition processes.
	 * @param tableConf Configuration.
	 * @return the config row count that one partition processes.
	 */
	public static long getInferRowCountPerPartition(Configuration tableConf) {
		return tableConf.getLong(
				TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION);
	}

	/**
	 * Gets default parallelism of operator.
	 * @param tableConf Configuration.
	 * @return default parallelism of operator.
	 */
	public static int getOperatorDefaultParallelism(Configuration tableConf, int envParallelism) {
		int parallelism = tableConf.getInteger(
				TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM);
		if (parallelism <= 0) {
			parallelism = envParallelism;
		}
		return parallelism;
	}

	/**
	 * Infer resource mode.
	 */
	public enum InferMode {
		NONE, ONLY_SOURCE
	}

	public static InferMode getInferMode(Configuration tableConf) {
		String config = tableConf.getString(
				TableConfigOptions.SQL_RESOURCE_INFER_MODE);
		try {
			return InferMode.valueOf(config);
		} catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException("Infer mode can only be set: NONE or ONLY_SOURCE.");
		}
	}

}
