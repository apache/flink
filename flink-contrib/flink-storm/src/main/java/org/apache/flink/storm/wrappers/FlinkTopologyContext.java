/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.wrappers;

import clojure.lang.Atom;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.state.ISubscribedState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * {@link FlinkTopologyContext} is a {@link TopologyContext} that overwrites certain method that are not applicable when
 * a Storm topology is executed within Flink.
 */
final class FlinkTopologyContext extends TopologyContext {

	/**
	 * Instantiates a new {@link FlinkTopologyContext} for a given Storm topology. The context object is instantiated
	 * for each parallel task
	 */
	FlinkTopologyContext(final StormTopology topology,
			@SuppressWarnings("rawtypes") final Map stormConf,
			final Map<Integer, String> taskToComponent, final Map<String, List<Integer>> componentToSortedTasks,
			final Map<String, Map<String, Fields>> componentToStreamToFields, final String stormId, final String codeDir,
			final String pidDir, final Integer taskId, final Integer workerPort, final List<Integer> workerTasks,
			final Map<String, Object> defaultResources, final Map<String, Object> userResources,
			final Map<String, Object> executorData, @SuppressWarnings("rawtypes") final Map registeredMetrics,
			final Atom openOrPrepareWasCalled) {
		super(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId,
				codeDir, pidDir, taskId, workerPort, workerTasks, defaultResources, userResources, executorData,
				registeredMetrics, openOrPrepareWasCalled);
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public void addTaskHook(final ITaskHook hook) {
		throw new UnsupportedOperationException("Task hooks are not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public Collection<ITaskHook> getHooks() {
		throw new UnsupportedOperationException("Task hooks are not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public IMetric getRegisteredMetricByName(final String name) {
		throw new UnsupportedOperationException("Metrics are not supported by Flink");

	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public CombinedMetric registerMetric(final String name, final ICombiner combiner, final int timeBucketSizeInSecs) {
		throw new UnsupportedOperationException("Metrics are not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public ReducedMetric registerMetric(final String name, final IReducer combiner, final int timeBucketSizeInSecs) {
		throw new UnsupportedOperationException("Metrics are not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public <T extends IMetric> T registerMetric(final String name, final T metric, final int timeBucketSizeInSecs) {
		throw new UnsupportedOperationException("Metrics are not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public <T extends ISubscribedState> T setAllSubscribedState(final T obj) {
		throw new UnsupportedOperationException("Not supported by Flink");

	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public <T extends ISubscribedState> T setSubscribedState(final String componentId, final T obj) {
		throw new UnsupportedOperationException("Not supported by Flink");
	}

	/**
	 * Not supported by Flink.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public <T extends ISubscribedState> T setSubscribedState(final String componentId, final String streamId, final T
			obj) {
		throw new UnsupportedOperationException("Not supported by Flink");
	}

}
