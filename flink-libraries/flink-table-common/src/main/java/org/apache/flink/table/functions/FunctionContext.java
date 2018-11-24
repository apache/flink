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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

import java.io.File;

/**
 * A FunctionContext allows to obtain global runtime information about the context in which the
 * user-defined function is executed. The information include the metric group,
 * the distributed cache files, and the global job parameters.
 */
@PublicEvolving
public class FunctionContext {

	/**
	 * @param context the runtime context in which the Flink Function is executed
	 */
	private RuntimeContext context;

	public FunctionContext(RuntimeContext context) {
		this.context = context;
	}

	/**
	 * Returns the metric group for this parallel subtask.
	 *
	 * @return metric group for this parallel subtask.
	 */
	public MetricGroup getMetricGroup() {
		return context.getMetricGroup();
	}

	/**
	 * Gets the local temporary file copy of a distributed cache files.
	 *
	 * @param name distributed cache file name
	 * @return local temporary file copy of a distributed cache file.
	 */
	public File getCachedFile(String name) {
		return context.getDistributedCache().getFile(name);
	}

	/**
	 * Gets the global job parameter value associated with the given key as a string.
	 *
	 * @param key          key pointing to the associated value
	 * @param defaultValue default value which is returned in case global job parameter is null
	 *                     or there is no value associated with the given key
	 * @return (default) value associated with the given key
	 */
	public String getJobParameter(String key, String defaultValue) {
		GlobalJobParameters conf = context.getExecutionConfig().getGlobalJobParameters();
		if (conf != null && conf.toMap().containsKey(key)) {
			return conf.toMap().get(key);
		} else {
			return defaultValue;
		}
	}
}
