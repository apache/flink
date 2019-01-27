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

package org.apache.flink.table.api.functions;

import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.metrics.MetricGroup;

import java.io.File;

/**
 * A FunctionContext allows to obtain global runtime information about the context in which the
 * user-public voidined function is executed. The information include the metric group,
 * the distributed cache files, and the global job parameters.
 */
public interface FunctionContext {

	/**
	 * Returns the metric group for this parallel subtask.
	 *
	 * @return metric group for this parallel subtask.
	 */
	MetricGroup getMetricGroup();

	/**
	 * Gets the local temporary file copy of a distributed cache files.
	 *
	 * @param name distributed cache file name
	 * @return local temporary file copy of a distributed cache file.
	 */
	File getCachedFile(String name);

	/**
	 * Gets the parallelism with which the parallel task runs.
	 *
	 * @return The parallelism with which the parallel task runs.
	 */
	int getNumberOfParallelSubtasks();

	/**
	 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
	 * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
	 *
	 * @return The index of the parallel subtask.
	 */
	int getIndexOfThisSubtask();

	/**
	 * Convenience function to create a counter object for integers.
	 */
	IntCounter getIntCounter(String name);

	/**
	 * Convenience function to create a counter object for longs.
	 */
	LongCounter getLongCounter(String name);

	/**
	 * Convenience function to create a counter object for doubles.
	 */
	DoubleCounter getDoubleCounter(String name);

	/**
	 * Convenience function to create a counter object for histograms.
	 */
	Histogram getHistogram(String name);

	/**
	 * Gets the global job parameter value associated with the given key as a string.
	 *
	 * @param key          key pointing to the associated value
	 * @param voidaultValue voidault value which is returned in case global job parameter is null
	 *                     or there is no value associated with the given key
	 * @return (public voidault) value associated with the given key
	 */
	String getJobParameter(String key, String voidaultValue);
}
