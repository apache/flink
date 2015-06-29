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

package org.apache.flink.stormcompatibility.api;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.stormcompatibility.wrappers.StormBoltWrapper;
import org.apache.flink.stormcompatibility.wrappers.StormSpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * {@link FlinkTopologyBuilder} mimics a {@link TopologyBuilder}, but builds a Flink program instead of a Storm
 * topology. Most methods (except {@link #createTopology()} are copied from the original {@link TopologyBuilder}
 * implementation to ensure equal behavior.<br />
 * <br />
 * <strong>CAUTION: {@link IRichStateSpout StateSpout}s and multiple output streams per spout/bolt are currently not
 * supported.</strong>
 */
public class FlinkTopologyBuilder {

	/** A Storm {@link TopologyBuilder} to build a real Storm topology */
	private final TopologyBuilder stormBuilder = new TopologyBuilder();
	/** All user spouts by their ID */
	private final HashMap<String, IRichSpout> spouts = new HashMap<String, IRichSpout>();
	/** All user bolts by their ID */
	private final HashMap<String, IRichBolt> bolts = new HashMap<String, IRichBolt>();
	/** All declared output schemas by operator ID */
	private final HashMap<String, Fields> outputSchemas = new HashMap<String, Fields>();

	/**
	 * Creates a Flink program that used the specified spouts and bolts.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public FlinkTopology createTopology() {
		final StormTopology stormTopolgoy = this.stormBuilder.createTopology();
		final FlinkTopology env = new FlinkTopology(stormTopolgoy);
		env.setParallelism(1);

		final HashMap<String, SingleOutputStreamOperator> availableOperators =
				new HashMap<String, SingleOutputStreamOperator>();

		for (final Entry<String, IRichSpout> spout : this.spouts.entrySet()) {
			final String spoutId = spout.getKey();
			final IRichSpout userSpout = spout.getValue();

			final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
			userSpout.declareOutputFields(declarer);
			this.outputSchemas.put(spoutId, declarer.outputSchema);

			/* TODO in order to support multiple output streams, use an additional wrapper (or modify StormSpoutWrapper
			 * and StormCollector)
			 * -> add an additional output attribute tagging the output stream, and use .split() and .select() to split
			 * the streams
			 */
			final DataStreamSource source = env.addSource(new StormSpoutWrapper(userSpout), declarer.getOutputType());
			availableOperators.put(spoutId, source);

			int dop = 1;
			final ComponentCommon common = stormTopolgoy.get_spouts().get(spoutId).get_common();
			if (common.is_set_parallelism_hint()) {
				dop = common.get_parallelism_hint();
				source.setParallelism(dop);
			}
			env.increaseNumberOfTasks(dop);
		}

		final HashMap<String, IRichBolt> unprocessedBolts = new HashMap<String, IRichBolt>();
		unprocessedBolts.putAll(this.bolts);

		final HashMap<String, Set<Entry<GlobalStreamId, Grouping>>> unprocessdInputsPerBolt =
				new HashMap<String, Set<Entry<GlobalStreamId, Grouping>>>();

		/* Because we do not know the order in which an iterator steps over a set, we might process a consumer before
		 * its producer
		 * ->thus, we might need to repeat multiple times
		 */
		while (unprocessedBolts.size() > 0) {

			final Iterator<Entry<String, IRichBolt>> boltsIterator = unprocessedBolts.entrySet().iterator();
			while (boltsIterator.hasNext()) {

				final Entry<String, IRichBolt> bolt = boltsIterator.next();
				final String boltId = bolt.getKey();
				final IRichBolt userBolt = bolt.getValue();

				final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
				userBolt.declareOutputFields(declarer);
				this.outputSchemas.put(boltId, declarer.outputSchema);

				final ComponentCommon common = stormTopolgoy.get_bolts().get(boltId).get_common();

				Set<Entry<GlobalStreamId, Grouping>> unprocessedInputs = unprocessdInputsPerBolt.get(boltId);
				if (unprocessedInputs == null) {
					unprocessedInputs = new HashSet<Entry<GlobalStreamId, Grouping>>();
					unprocessedInputs.addAll(common.get_inputs().entrySet());
					unprocessdInputsPerBolt.put(boltId, unprocessedInputs);
				}

				// connect each available producer to the current bolt
				final Iterator<Entry<GlobalStreamId, Grouping>> inputStreamsIterator = unprocessedInputs.iterator();
				while (inputStreamsIterator.hasNext()) {

					final Entry<GlobalStreamId, Grouping> inputStream = inputStreamsIterator.next();
					final String producerId = inputStream.getKey().get_componentId();

					DataStream<?> inputDataStream = availableOperators.get(producerId);

					if (inputDataStream != null) {
						// if producer was processed already
						final Grouping grouping = inputStream.getValue();
						if (grouping.is_set_shuffle()) {
							// Storm uses a round-robin shuffle strategy
							inputDataStream = inputDataStream.rebalance();
						} else if (grouping.is_set_fields()) {
							// global grouping is emulated in Storm via an empty fields grouping list
							final List<String> fields = grouping.get_fields();
							if (fields.size() > 0) {
								inputDataStream = inputDataStream.groupBy(declarer.getGroupingFieldIndexes(grouping
										.get_fields()));
							} else {
								inputDataStream = inputDataStream.global();
							}
						} else if (grouping.is_set_all()) {
							inputDataStream = inputDataStream.broadcast();
						} else if (!grouping.is_set_local_or_shuffle()) {
							throw new UnsupportedOperationException(
									"Flink only supports (local-or-)shuffle, fields, all, and global grouping");
						}

						final TypeInformation<?> outType = declarer.getOutputType();

						final SingleOutputStreamOperator operator = inputDataStream.transform(boltId, outType,
								new StormBoltWrapper(userBolt, this.outputSchemas.get(producerId)));
						if (outType != null) {
							// only for non-sink nodes
							availableOperators.put(boltId, operator);
						}

						int dop = 1;
						if (common.is_set_parallelism_hint()) {
							dop = common.get_parallelism_hint();
							operator.setParallelism(dop);
						}
						env.increaseNumberOfTasks(dop);

						inputStreamsIterator.remove();
					}
				}

				if (unprocessedInputs.size() == 0) {
					// all inputs are connected; processing bolt completed
					boltsIterator.remove();
				}
			}
		}
		return env;
	}

	/**
	 * Define a new bolt in this topology with parallelism of just one thread.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this bolt's
	 * 		outputs.
	 * @param bolt
	 * 		the bolt
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(final String id, final IRichBolt bolt) {
		return this.setBolt(id, bolt, null);
	}

	/**
	 * Define a new bolt in this topology with the specified amount of parallelism.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this bolt's
	 * 		outputs.
	 * @param bolt
	 * 		the bolt
	 * @param parallelism_hint
	 * 		the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a
	 * 		process somewhere around the cluster.
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(final String id, final IRichBolt bolt, final Number parallelism_hint) {
		final BoltDeclarer declarer = this.stormBuilder.setBolt(id, bolt, parallelism_hint);
		this.bolts.put(id, bolt);
		return declarer;
	}

	/**
	 * Define a new bolt in this topology. This defines a basic bolt, which is a simpler to use but more restricted
	 * kind
	 * of bolt. Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to
	 * achieve proper reliability in the topology.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this bolt's
	 * 		outputs.
	 * @param bolt
	 * 		the basic bolt
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(final String id, final IBasicBolt bolt) {
		return this.setBolt(id, bolt, null);
	}

	/**
	 * Define a new bolt in this topology. This defines a basic bolt, which is a simpler to use but more restricted
	 * kind
	 * of bolt. Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to
	 * achieve proper reliability in the topology.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this bolt's
	 * 		outputs.
	 * @param bolt
	 * 		the basic bolt
	 * @param parallelism_hint
	 * 		the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a
	 * 		process somwehere around the cluster.
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(final String id, final IBasicBolt bolt, final Number parallelism_hint) {
		return this.setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
	}

	/**
	 * Define a new spout in this topology.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this spout's
	 * 		outputs.
	 * @param spout
	 * 		the spout
	 */
	public SpoutDeclarer setSpout(final String id, final IRichSpout spout) {
		return this.setSpout(id, spout, null);
	}

	/**
	 * Define a new spout in this topology with the specified parallelism. If the spout declares itself as
	 * non-distributed, the parallelism_hint will be ignored and only one task will be allocated to this component.
	 *
	 * @param id
	 * 		the id of this component. This id is referenced by other components that want to consume this spout's
	 * 		outputs.
	 * @param parallelism_hint
	 * 		the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a
	 * 		process somwehere around the cluster.
	 * @param spout
	 * 		the spout
	 */
	public SpoutDeclarer setSpout(final String id, final IRichSpout spout, final Number parallelism_hint) {
		final SpoutDeclarer declarer = this.stormBuilder.setSpout(id, spout, parallelism_hint);
		this.spouts.put(id, spout);
		return declarer;
	}

	// TODO add StateSpout support (Storm 0.9.4 does not yet support StateSpouts itself)
	/* not implemented by Storm 0.9.4
	 * public void setStateSpout(final String id, final IRichStateSpout stateSpout) {
	 * this.stormBuilder.setStateSpout(id, stateSpout);
	 * }
	 * public void setStateSpout(final String id, final IRichStateSpout stateSpout, final Number parallelism_hint) {
	 * this.stormBuilder.setStateSpout(id, stateSpout, parallelism_hint);
	 * }
	 */

}
