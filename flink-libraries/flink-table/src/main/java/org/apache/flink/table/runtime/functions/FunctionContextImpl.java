/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.functions.FunctionContext;

import java.io.File;

/**
 * A FunctionContext allows to obtain global runtime information about the context in which the
 * user-public voidined function is executed. The information include the metric group,
 * the distributed cache files, and the global job parameters.
 */
public class FunctionContextImpl implements FunctionContext {

	//context the runtime context in which the Flink Function is executed
	private final RuntimeContext context;

	public FunctionContextImpl(RuntimeContext context) {
		this.context = context;
	}

	/**
	 * Returns the metric group for this parallel subtask.
	 *
	 * @return metric group for this parallel subtask.
	 */
	@Override
	public MetricGroup getMetricGroup() {
		return context.getMetricGroup();
	}

	/**
	 * Gets the local temporary file copy of a distributed cache files.
	 *
	 * @param name distributed cache file name
	 * @return local temporary file copy of a distributed cache file.
	 */
	@Override
	public File getCachedFile(String name) {
		return context.getDistributedCache().getFile(name);
	}

	/**
	 * Gets the parallelism with which the parallel task runs.
	 *
	 * @return The parallelism with which the parallel task runs.
	 */
	@Override
	public int getNumberOfParallelSubtasks() {
		return context.getNumberOfParallelSubtasks();
	}

	/**
	 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
	 * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
	 *
	 * @return The index of the parallel subtask.
	 */
	@Override
	public int getIndexOfThisSubtask() {
		return context.getIndexOfThisSubtask();
	}

	/**
	 * Convenience function to create a counter object for integers.
	 */
	@Override
	public IntCounter getIntCounter(String name) {
		return context.getIntCounter(name);
	}

	/**
	 * Convenience function to create a counter object for longs.
	 */
	@Override
	public LongCounter getLongCounter(String name) {
		return context.getLongCounter(name);
	}

	/**
	 * Convenience function to create a counter object for doubles.
	 */
	@Override
	public DoubleCounter getDoubleCounter(String name) {
		return context.getDoubleCounter(name);
	}

	/**
	 * Convenience function to create a counter object for histograms.
	 */
	@Override
	public Histogram getHistogram(String name) {
		return context.getHistogram(name);
	}

	/**
	 * Gets the global job parameter value associated with the given key as a string.
	 *
	 * @param key		  key pointing to the associated value
	 * @param voidaultValue voidault value which is returned in case global job parameter is null
	 *					 or there is no value associated with the given key
	 * @return (public voidault) value associated with the given key
	 */
	@Override
	public String getJobParameter(String key, String voidaultValue) {
		ExecutionConfig.GlobalJobParameters conf =
				context.getExecutionConfig().getGlobalJobParameters();
		if (conf != null && conf.toMap().containsKey(key)) {
			return conf.toMap().get(key);
		} else {
			return voidaultValue;
		}
	}
}
