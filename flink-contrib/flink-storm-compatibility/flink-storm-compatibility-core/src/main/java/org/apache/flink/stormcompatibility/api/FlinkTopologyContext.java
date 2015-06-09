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

package org.apache.flink.stormcompatibility.api;

import backtype.storm.generated.StormTopology;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.state.ISubscribedState;
import backtype.storm.task.TopologyContext;

import java.util.Collection;
import java.util.Map;

/**
 * {@link FlinkTopologyContext} is a {@link TopologyContext} that overwrites certain method that are not applicable when
 * a Storm topology is executed within Flink.
 */
public class FlinkTopologyContext extends TopologyContext {

	/**
	 * Instantiates a new {@link FlinkTopologyContext} for a given Storm topology. The context object is instantiated
	 * for each parallel task
	 *
	 * @param topology
	 * 		The Storm topology that is currently executed
	 * @param taskToComponents
	 * 		A map from task IDs to Component IDs
	 * @param taskId
	 * 		The ID of the task the context belongs to.
	 */
	public FlinkTopologyContext(final StormTopology topology, final Map<Integer, String> taskToComponents,
			final Integer taskId) {
		super(topology, null, taskToComponents, null, null, null, null, null, taskId, null, null, null, null, null,
				null, null);
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
	@SuppressWarnings("unchecked")
	@Override
	public IMetric registerMetric(final String name, final IMetric metric, final int timeBucketSizeInSecs) {
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
