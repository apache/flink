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

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import clojure.lang.Atom;
import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link WrapperSetupHelper} is an helper class used by {@link SpoutWrapper} and
 * {@link BoltWrapper}.
 */
class WrapperSetupHelper {

	/** The configuration key for the topology name. */
	static final String TOPOLOGY_NAME = "storm.topology.name";

	/**
	 * Computes the number of output attributes used by a {@link SpoutWrapper} or {@link BoltWrapper}
	 * per declared output stream. The number is {@code -1} for raw output type or a value within range [0;25] for
	 * output type {@link org.apache.flink.api.java.tuple.Tuple0 Tuple0} to
	 * {@link org.apache.flink.api.java.tuple.Tuple25 Tuple25}.
	 *
	 * @param spoutOrBolt
	 *            The Storm {@link IRichSpout spout} or {@link IRichBolt bolt} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type
	 *            {@link org.apache.flink.api.java.tuple.Tuple1 Tuple1} but be of a raw type. (Can be {@code null}.)
	 * @return The number of attributes to be used for each stream.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	static HashMap<String, Integer> getNumberOfAttributes(final IComponent spoutOrBolt,
			final Collection<String> rawOutputs)
					throws IllegalArgumentException {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		spoutOrBolt.declareOutputFields(declarer);

		for (Entry<String, Integer> schema : declarer.outputSchemas.entrySet()) {
			int declaredNumberOfAttributes = schema.getValue();
			if ((declaredNumberOfAttributes < 0) || (declaredNumberOfAttributes > 25)) {
				throw new IllegalArgumentException(
						"Provided bolt declares non supported number of output attributes. Must be in range [0;25] but "
								+ "was " + declaredNumberOfAttributes);
			}

			if (rawOutputs != null && rawOutputs.contains(schema.getKey())) {
				if (declaredNumberOfAttributes != 1) {
					throw new IllegalArgumentException(
							"Ouput type is requested to be raw type, but provided bolt declares more then one output "
									+ "attribute.");
				}
				schema.setValue(-1);
			}
		}

		return declarer.outputSchemas;
	}

	/** Used to compute unique task IDs for a Storm topology. */
	private static int tid;

	/**
	 * Creates a {@link TopologyContext} for a Spout or Bolt instance (ie, Flink task / Storm executor).
	 *
	 * @param context
	 *            The Flink runtime context.
	 * @param spoutOrBolt
	 *            The Spout or Bolt this context is created for.
	 * @param stormTopology
	 *            The original Storm topology.
	 * @param stormConfig
	 *            The user provided configuration.
	 * @return The created {@link TopologyContext}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static synchronized TopologyContext createTopologyContext(
			final StreamingRuntimeContext context, final IComponent spoutOrBolt,
			final String operatorName, StormTopology stormTopology, final Map stormConfig) {

		final int dop = context.getNumberOfParallelSubtasks();

		final Map<Integer, String> taskToComponents = new HashMap<Integer, String>();
		final Map<String, List<Integer>> componentToSortedTasks = new HashMap<String, List<Integer>>();
		final Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();
		String stormId = (String) stormConfig.get(TOPOLOGY_NAME);
		String codeDir = null; // not supported
		String pidDir = null; // not supported
		Integer taskId = -1;
		Integer workerPort = null; // not supported
		List<Integer> workerTasks = new ArrayList<Integer>();
		final Map<String, Object> defaultResources = new HashMap<String, Object>();
		final Map<String, Object> userResources = new HashMap<String, Object>();
		final Map<String, Object> executorData = new HashMap<String, Object>();
		final Map registeredMetrics = new HashMap();
		Atom openOrPrepareWasCalled = null;

		if (stormTopology == null) {
			// embedded mode
			ComponentCommon common = new ComponentCommon();
			common.set_parallelism_hint(dop);

			HashMap<String, SpoutSpec> spouts = new HashMap<String, SpoutSpec>();
			HashMap<String, Bolt> bolts = new HashMap<String, Bolt>();
			if (spoutOrBolt instanceof IRichSpout) {
				spouts.put(operatorName, new SpoutSpec(null, common));
			} else {
				assert (spoutOrBolt instanceof IRichBolt);
				bolts.put(operatorName, new Bolt(null, common));
			}
			stormTopology = new StormTopology(spouts, bolts, new HashMap<String, StateSpoutSpec>());

			List<Integer> sortedTasks = new ArrayList<Integer>(dop);
			for (int i = 1; i <= dop; ++i) {
				taskToComponents.put(i, operatorName);
				sortedTasks.add(i);
			}
			componentToSortedTasks.put(operatorName, sortedTasks);

			SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
			spoutOrBolt.declareOutputFields(declarer);
			componentToStreamToFields.put(operatorName, declarer.outputStreams);
		} else {
			// whole topology is built (i.e. FlinkTopology is used)
			Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
			Map<String, Bolt> bolts = stormTopology.get_bolts();
			Map<String, StateSpoutSpec> stateSpouts = stormTopology.get_state_spouts();

			tid = 1;

			for (Entry<String, SpoutSpec> spout : spouts.entrySet()) {
				Integer rc = processSingleOperator(spout.getKey(), spout.getValue().get_common(),
						operatorName, context.getIndexOfThisSubtask(), dop, taskToComponents,
						componentToSortedTasks, componentToStreamToFields);
				if (rc != null) {
					taskId = rc;
				}
			}
			for (Entry<String, Bolt> bolt : bolts.entrySet()) {
				Integer rc = processSingleOperator(bolt.getKey(), bolt.getValue().get_common(),
						operatorName, context.getIndexOfThisSubtask(), dop, taskToComponents,
						componentToSortedTasks, componentToStreamToFields);
				if (rc != null) {
					taskId = rc;
				}
			}
			for (Entry<String, StateSpoutSpec> stateSpout : stateSpouts.entrySet()) {
				Integer rc = processSingleOperator(stateSpout.getKey(), stateSpout
						.getValue().get_common(), operatorName, context.getIndexOfThisSubtask(),
						dop, taskToComponents, componentToSortedTasks, componentToStreamToFields);
				if (rc != null) {
					taskId = rc;
				}
			}

			assert(taskId != null);
		}

		if (!stormConfig.containsKey(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
			stormConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30); // Storm default value
		}

		return new FlinkTopologyContext(stormTopology, stormConfig, taskToComponents,
				componentToSortedTasks, componentToStreamToFields, stormId, codeDir, pidDir,
				taskId, workerPort, workerTasks, defaultResources, userResources, executorData,
				registeredMetrics, openOrPrepareWasCalled);
	}

	/**
	 * Sets up {@code taskToComponents}, {@code componentToSortedTasks}, and {@code componentToStreamToFields} for a
	 * single instance of a Spout or Bolt (ie, task or executor). Furthermore, is computes the unique task-id.
	 *
	 * @param componentId
	 *            The ID of the Spout/Bolt in the topology.
	 * @param common
	 *            The common operator object (that is all Spouts and Bolts have).
	 * @param operatorName
	 *            The Flink operator name.
	 * @param index
	 *            The index of the currently processed tasks with its operator.
	 * @param dop
	 *            The parallelism of the operator.
	 * @param taskToComponents
	 *            OUTPUT: A map from all task IDs of the topology to their component IDs.
	 * @param componentToSortedTasks
	 *            OUTPUT: A map from all component IDs to their sorted list of corresponding task IDs.
	 * @param componentToStreamToFields
	 *            OUTPUT: A map from all component IDs to there output streams and output fields.
	 *
	 * @return A unique task ID if the currently processed Spout or Bolt ({@code componentId}) is equal to the current
	 *         Flink operator {@code operatorName} -- {@code null} otherwise.
	 */
	private static Integer processSingleOperator(final String componentId,
			final ComponentCommon common, final String operatorName, final int index,
			final int dop, final Map<Integer, String> taskToComponents,
			final Map<String, List<Integer>> componentToSortedTasks,
			final Map<String, Map<String, Fields>> componentToStreamToFields) {
		final int parallelismHint = common.get_parallelism_hint();
		Integer taskId = null;

		if (componentId.equals(operatorName)) {
			taskId = tid + index;
		}

		List<Integer> sortedTasks = new ArrayList<Integer>(dop);
		for (int i = 0; i < parallelismHint; ++i) {
			taskToComponents.put(tid, componentId);
			sortedTasks.add(tid);
			++tid;
		}
		componentToSortedTasks.put(componentId, sortedTasks);

		Map<String, Fields> outputStreams = new HashMap<String, Fields>();
		for (Entry<String, StreamInfo> outStream : common.get_streams().entrySet()) {
			outputStreams.put(outStream.getKey(), new Fields(outStream.getValue().get_output_fields()));
		}
		componentToStreamToFields.put(componentId, outputStreams);

		return taskId;
	}

}
