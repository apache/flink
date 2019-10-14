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

package org.apache.flink.table.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;

/**
 * Utilities for merging the table config parameters and global job parameters.
 */
public class GlobalJobParametersMerger {

	private GlobalJobParametersMerger() {}

	/**
	 * Merge global job parameters and table config parameters,
	 * and set the merged result to GlobalJobParameters.
	 */
	public static void mergeParameters(ExecutionConfig executionConfig, TableConfig tableConfig) {
		Configuration parameters = new Configuration();
		parameters.addAll(tableConfig.getConfiguration());
		executionConfig.getGlobalJobParameters().toMap().forEach(parameters::setString);
		executionConfig.setGlobalJobParameters(parameters);
	}
}
