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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.stormcompatibility.util.FlinkStormStreamSelector;
import org.apache.flink.stormcompatibility.util.SplitStreamType;
import org.apache.flink.stormcompatibility.wrappers.AbstractStormSpoutWrapper;
import org.apache.flink.stormcompatibility.wrappers.FiniteStormSpout;
import org.apache.flink.stormcompatibility.wrappers.FiniteStormSpoutWrapper;
import org.apache.flink.stormcompatibility.wrappers.StormBoltWrapper;
import org.apache.flink.stormcompatibility.wrappers.StormSpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;

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
 * <strong>CAUTION: {@link IRichStateSpout StateSpout}s are currently not supported.</strong>
 */
public class FlinkTopologyBuilder {

	/** A Storm {@link TopologyBuilder} to build a real Storm topology */
	private final TopologyBuilder stormBuilder = new TopologyBuilder();
	/** All user spouts by their ID */
	private final HashMap<String, IRichSpout> spouts = new HashMap<String, IRichSpout>();
	/** All user bolts by their ID */
	private final HashMap<String, IRichBolt> bolts = new HashMap<String, IRichBolt>();
	/** All declared streams and output schemas by operator ID */
	private final HashMap<String, HashMap<String, Fields>> outputStreams = new HashMap<String, HashMap<String, Fields>>();
	/** All spouts&bolts declarers by their ID */
	private final HashMap<String, FlinkOutputFieldsDeclarer> declarers = new HashMap<String, FlinkOutputFieldsDeclarer>();

	/**
	 * Creates a Flink program that uses the specified spouts and bolts.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public FlinkTopology createTopology() {
		final StormTopology stormTopolgoy = this.stormBuilder.createTopology();
		final FlinkTopology env = new FlinkTopology(stormTopolgoy);
		env.setParallelism(1);

		final HashMap<String, HashMap<String, DataStream>> availableInputs = new HashMap<String, HashMap<String, DataStream>>();

		for (final Entry<String, IRichSpout> spout : this.spouts.entrySet()) {
			final String spoutId = spout.getKey();
			final IRichSpout userSpout = spout.getValue();

			final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
			userSpout.declareOutputFields(declarer);
			final HashMap<String,Fields> sourceStreams = declarer.outputStreams;
			this.outputStreams.put(spoutId, sourceStreams);
			declarers.put(spoutId, declarer);

			AbstractStormSpoutWrapper spoutWrapper;

			if (userSpout instanceof FiniteStormSpout) {
				spoutWrapper = new FiniteStormSpoutWrapper((FiniteStormSpout) userSpout);
			} else {
				spoutWrapper = new StormSpoutWrapper(userSpout);
			}

			DataStreamSource source;
			HashMap<String, DataStream> outputStreams = new HashMap<String, DataStream>();
			if (sourceStreams.size() == 1) {
				final String outputStreamId = (String) sourceStreams.keySet().toArray()[0];
				source = env.addSource(spoutWrapper, spoutId,
						declarer.getOutputType(outputStreamId));
				outputStreams.put(outputStreamId, source);
			} else {
				source = env.addSource(spoutWrapper, spoutId,
						TypeExtractor.getForClass(SplitStreamType.class));
				SplitDataStream splitSource = source.split(new FlinkStormStreamSelector());

				for (String streamId : sourceStreams.keySet()) {
					outputStreams.put(streamId, splitSource.select(streamId));
				}
			}
			availableInputs.put(spoutId, outputStreams);

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
		boolean makeProgress = true;
		while (unprocessedBolts.size() > 0) {
			if (!makeProgress) {
				throw new RuntimeException(
						"Unable to build Topology. Could not connect the following bolts: "
								+ unprocessedBolts.keySet());
			}
			makeProgress = false;

			final Iterator<Entry<String, IRichBolt>> boltsIterator = unprocessedBolts.entrySet().iterator();
			while (boltsIterator.hasNext()) {

				final Entry<String, IRichBolt> bolt = boltsIterator.next();
				final String boltId = bolt.getKey();
				final IRichBolt userBolt = bolt.getValue();

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

					final Entry<GlobalStreamId, Grouping> stormInputStream = inputStreamsIterator.next();
					final String producerId = stormInputStream.getKey().get_componentId();
					final String inputStreamId = stormInputStream.getKey().get_streamId();

					HashMap<String, DataStream> producer = availableInputs.get(producerId);
					if (producer != null) {
						makeProgress = true;

						DataStream inputStream = producer.get(inputStreamId);
						if (inputStream != null) {
							final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
							userBolt.declareOutputFields(declarer);
							final HashMap<String, Fields> boltOutputStreams = declarer.outputStreams;
							this.outputStreams.put(boltId, boltOutputStreams);
							this.declarers.put(boltId, declarer);

							// if producer was processed already
							final Grouping grouping = stormInputStream.getValue();
							if (grouping.is_set_shuffle()) {
								// Storm uses a round-robin shuffle strategy
								inputStream = inputStream.rebalance();
							} else if (grouping.is_set_fields()) {
								// global grouping is emulated in Storm via an empty fields grouping list
								final List<String> fields = grouping.get_fields();
								if (fields.size() > 0) {
									FlinkOutputFieldsDeclarer prodDeclarer = this.declarers.get(producerId);
									inputStream = inputStream.groupBy(prodDeclarer
											.getGroupingFieldIndexes(inputStreamId,
													grouping.get_fields()));
								} else {
									inputStream = inputStream.global();
								}
							} else if (grouping.is_set_all()) {
								inputStream = inputStream.broadcast();
							} else if (!grouping.is_set_local_or_shuffle()) {
								throw new UnsupportedOperationException(
										"Flink only supports (local-or-)shuffle, fields, all, and global grouping");
							}

							SingleOutputStreamOperator outputStream;
							if (boltOutputStreams.size() < 2) { // single output stream or sink
								String outputStreamId = null;
								if (boltOutputStreams.size() == 1) {
									outputStreamId = (String) boltOutputStreams.keySet().toArray()[0];
								}
								final TypeInformation<?> outType = declarer
										.getOutputType(outputStreamId);

								outputStream = inputStream.transform(
										boltId,
										outType,
										new StormBoltWrapper(userBolt, this.outputStreams.get(
												producerId).get(inputStreamId)));

								if (outType != null) {
									// only for non-sink nodes
									HashMap<String, DataStream> op = new HashMap<String, DataStream>();
									op.put(outputStreamId, outputStream);
									availableInputs.put(boltId, op);
								}
							} else {
								final TypeInformation<?> outType = TypeExtractor
										.getForClass(SplitStreamType.class);

								outputStream = inputStream.transform(
										boltId,
										outType,
										new StormBoltWrapper(userBolt, this.outputStreams.get(
												producerId).get(inputStreamId)));

								SplitDataStream splitStreams = outputStream
										.split(new FlinkStormStreamSelector());

								HashMap<String, DataStream> op = new HashMap<String, DataStream>();
								for (String outputStreamId : boltOutputStreams.keySet()) {
									op.put(outputStreamId, splitStreams.select(outputStreamId));
								}
								availableInputs.put(boltId, op);
							}

							int dop = 1;
							if (common.is_set_parallelism_hint()) {
								dop = common.get_parallelism_hint();
								outputStream.setParallelism(dop);
							}
							env.increaseNumberOfTasks(dop);

							inputStreamsIterator.remove();
						} else {
							throw new RuntimeException("Cannot connect '" + boltId + "' to '"
									+ producerId + "'. Stream '" + inputStreamId + "' not found.");
						}
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
