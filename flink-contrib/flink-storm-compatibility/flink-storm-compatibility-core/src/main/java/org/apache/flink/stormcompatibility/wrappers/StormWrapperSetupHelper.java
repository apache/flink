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

package org.apache.flink.stormcompatibility.wrappers;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;

import org.apache.flink.stormcompatibility.api.FlinkTopologyContext;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link StormWrapperSetupHelper} is an helper class used by {@link AbstractStormSpoutWrapper} or
 * {@link StormBoltWrapper}.
 */
class StormWrapperSetupHelper {

	/**
	 * Computes the number of output attributes used by a {@link AbstractStormSpoutWrapper} or {@link StormBoltWrapper}
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
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public static HashMap<String, Integer> getNumberOfAttributes(final IComponent spoutOrBolt,
			final Collection<String> rawOutputs)
					throws IllegalArgumentException {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
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

	// TODO
	public static TopologyContext convertToTopologyContext(final StreamingRuntimeContext context,
			final boolean spoutOrBolt) {
		final Integer taskId = new Integer(1 + context.getIndexOfThisSubtask());

		final Map<Integer, String> taskToComponents = new HashMap<Integer, String>();
		taskToComponents.put(taskId, context.getTaskName());

		final ComponentCommon common = new ComponentCommon();
		common.set_parallelism_hint(context.getNumberOfParallelSubtasks());

		final Map<String, Bolt> bolts = new HashMap<String, Bolt>();
		final Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();

		if (spoutOrBolt) {
			spoutSpecs.put(context.getTaskName(), new SpoutSpec(null, common));
		} else {
			bolts.put(context.getTaskName(), new Bolt(null, common));
		}

		return new FlinkTopologyContext(new StormTopology(spoutSpecs, bolts, null), taskToComponents, taskId);
	}

}
