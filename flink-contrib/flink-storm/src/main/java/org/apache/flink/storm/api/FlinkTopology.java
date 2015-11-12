/*
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
package org.apache.flink.storm.api;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.storm.util.SplitStreamMapper;
import org.apache.flink.storm.util.SplitStreamType;
import org.apache.flink.storm.util.StormStreamSelector;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.storm.wrappers.BoltWrapperTwoInput;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * {@link FlinkTopology} translates a {@link TopologyBuilder} to a Flink program.
 * <strong>CAUTION: {@link IRichStateSpout StateSpout}s are currently not supported.</strong>
 */
public class FlinkTopology {

	/** All declared streams and output schemas by operator ID */
	private final HashMap<String, HashMap<String, Fields>> outputStreams = new HashMap<String, HashMap<String, Fields>>();
	/** All spouts&bolts declarers by their ID */
	private final HashMap<String, FlinkOutputFieldsDeclarer> declarers = new HashMap<String, FlinkOutputFieldsDeclarer>();

	private final HashMap<String, Set<Entry<GlobalStreamId, Grouping>>> unprocessdInputsPerBolt =
			new HashMap<String, Set<Entry<GlobalStreamId, Grouping>>>();

	final HashMap<String, HashMap<String, DataStream<Tuple>>> availableInputs = new HashMap<>();

	private final TopologyBuilder builder;

	// needs to be a class member for internal testing purpose
	private final StormTopology stormTopology;

	private final Map<String, IRichSpout> spouts;
	private final Map<String, IRichBolt> bolts;

	private final StreamExecutionEnvironment env;

	private FlinkTopology(TopologyBuilder builder) {
		this.builder = builder;
		this.stormTopology = builder.createTopology();
		// extract the spouts and bolts
		this.spouts = getPrivateField("_spouts");
		this.bolts = getPrivateField("_bolts");

		this.env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Kick off the translation immediately
		translateTopology();
	}

	/**
	 *
	 * Creates a Flink program that uses the specified spouts and bolts.
	 * @param stormBuilder The Storm topology builder to use for creating the Flink topology.
	 * @return A {@link FlinkTopology} which contains the translated Storm topology and may be executed.
	 */
	public static FlinkTopology createTopology(TopologyBuilder stormBuilder) {
		return new FlinkTopology(stormBuilder);
	}

	/**
	 * Returns the underlying Flink {@link StreamExecutionEnvironment} for the Storm topology.
	 * @return The contextual environment (local or remote).
	 */
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return this.env;
	}

	/**
	 * Directly executes the Storm topology based on the current context (local when in IDE and
	 * remote when executed through ./bin/flink).
	 * @return The Flink {@link JobExecutionResult} after the execution of the Storm topology.
	 * @throws Exception which occurs during execution of the translated Storm topology.
	 */
	public JobExecutionResult execute() throws Exception {
		return env.execute();
	}


	@SuppressWarnings("unchecked")
	private <T> Map<String, T> getPrivateField(String field) {
		try {
			Field f = builder.getClass().getDeclaredField(field);
			f.setAccessible(true);
			return copyObject((Map<String, T>) f.get(builder));
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Couldn't get " + field + " from TopologyBuilder", e);
		}
	}

	private <T> T copyObject(T object) {
		try {
			return InstantiationUtil.deserializeObject(
					InstantiationUtil.serializeObject(object),
					getClass().getClassLoader()
			);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Failed to copy object.");
		}
	}

	/**
	 * Creates a Flink program that uses the specified spouts and bolts.
	 */
	private void translateTopology() {

		unprocessdInputsPerBolt.clear();
		outputStreams.clear();
		declarers.clear();
		availableInputs.clear();

		// Storm defaults to parallelism 1
		env.setParallelism(1);

		/* Translation of topology */


		for (final Entry<String, IRichSpout> spout : spouts.entrySet()) {
			final String spoutId = spout.getKey();
			final IRichSpout userSpout = spout.getValue();

			final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
			userSpout.declareOutputFields(declarer);
			final HashMap<String,Fields> sourceStreams = declarer.outputStreams;
			this.outputStreams.put(spoutId, sourceStreams);
			declarers.put(spoutId, declarer);


			final HashMap<String, DataStream<Tuple>> outputStreams = new HashMap<String, DataStream<Tuple>>();
			final DataStreamSource<?> source;

			if (sourceStreams.size() == 1) {
				final SpoutWrapper<Tuple> spoutWrapperSingleOutput = new SpoutWrapper<Tuple>(userSpout, spoutId, null, null);
				spoutWrapperSingleOutput.setStormTopology(stormTopology);

				final String outputStreamId = (String) sourceStreams.keySet().toArray()[0];

				DataStreamSource<Tuple> src = env.addSource(spoutWrapperSingleOutput, spoutId,
						declarer.getOutputType(outputStreamId));

				outputStreams.put(outputStreamId, src);
				source = src;
			} else {
				final SpoutWrapper<SplitStreamType<Tuple>> spoutWrapperMultipleOutputs = new SpoutWrapper<SplitStreamType<Tuple>>(
						userSpout, spoutId, null, null);
				spoutWrapperMultipleOutputs.setStormTopology(stormTopology);

				@SuppressWarnings({ "unchecked", "rawtypes" })
				DataStreamSource<SplitStreamType<Tuple>> multiSource = env.addSource(
						spoutWrapperMultipleOutputs, spoutId,
						(TypeInformation) TypeExtractor.getForClass(SplitStreamType.class));

				SplitStream<SplitStreamType<Tuple>> splitSource = multiSource
						.split(new StormStreamSelector<Tuple>());
				for (String streamId : sourceStreams.keySet()) {
					SingleOutputStreamOperator<Tuple, ?> outStream = splitSource.select(streamId)
							.map(new SplitStreamMapper<Tuple>());
					outStream.getTransformation().setOutputType(declarer.getOutputType(streamId));
					outputStreams.put(streamId, outStream);
				}
				source = multiSource;
			}
			availableInputs.put(spoutId, outputStreams);

			final ComponentCommon common = stormTopology.get_spouts().get(spoutId).get_common();
			if (common.is_set_parallelism_hint()) {
				int dop = common.get_parallelism_hint();
				source.setParallelism(dop);
			} else {
				common.set_parallelism_hint(1);
			}
		}

		/**
		* 1. Connect all spout streams with bolts streams
		* 2. Then proceed with the bolts stream already connected
		*
		*  Because we do not know the order in which an iterator steps over a set, we might process a consumer before
		* its producer
		* ->thus, we might need to repeat multiple times
		*/
		boolean makeProgress = true;
		while (bolts.size() > 0) {
			if (!makeProgress) {
				throw new RuntimeException(
						"Unable to build Topology. Could not connect the following bolts: "
								+ bolts.keySet());
			}
			makeProgress = false;

			final Iterator<Entry<String, IRichBolt>> boltsIterator = bolts.entrySet().iterator();
			while (boltsIterator.hasNext()) {

				final Entry<String, IRichBolt> bolt = boltsIterator.next();
				final String boltId = bolt.getKey();
				final IRichBolt userBolt = copyObject(bolt.getValue());

				final ComponentCommon common = stormTopology.get_bolts().get(boltId).get_common();

				Set<Entry<GlobalStreamId, Grouping>> unprocessedBoltInputs = unprocessdInputsPerBolt.get(boltId);
				if (unprocessedBoltInputs == null) {
					unprocessedBoltInputs = new HashSet<>();
					unprocessedBoltInputs.addAll(common.get_inputs().entrySet());
					unprocessdInputsPerBolt.put(boltId, unprocessedBoltInputs);
				}

				// check if all inputs are available
				final int numberOfInputs = unprocessedBoltInputs.size();
				int inputsAvailable = 0;
				for (Entry<GlobalStreamId, Grouping> entry : unprocessedBoltInputs) {
					final String producerId = entry.getKey().get_componentId();
					final String streamId = entry.getKey().get_streamId();
					final HashMap<String, DataStream<Tuple>> streams = availableInputs.get(producerId);
					if (streams != null && streams.get(streamId) != null) {
						inputsAvailable++;
					}
				}

				if (inputsAvailable != numberOfInputs) {
					// traverse other bolts first until inputs are available
					continue;
				} else {
					makeProgress = true;
					boltsIterator.remove();
				}

				final Map<GlobalStreamId, DataStream<Tuple>> inputStreams = new HashMap<>(numberOfInputs);

				for (Entry<GlobalStreamId, Grouping> input : unprocessedBoltInputs) {
					final GlobalStreamId streamId = input.getKey();
					final Grouping grouping = input.getValue();

					final String producerId = streamId.get_componentId();

					final Map<String, DataStream<Tuple>> producer = availableInputs.get(producerId);

					inputStreams.put(streamId, processInput(boltId, userBolt, streamId, grouping, producer));
				}

				final Iterator<Entry<GlobalStreamId, DataStream<Tuple>>> iterator = inputStreams.entrySet().iterator();

				final Entry<GlobalStreamId, DataStream<Tuple>> firstInput = iterator.next();
				GlobalStreamId streamId = firstInput.getKey();
				DataStream<Tuple> inputStream = firstInput.getValue();

				final SingleOutputStreamOperator<?, ?> outputStream;

				switch (numberOfInputs) {
					case 1:
						outputStream = createOutput(boltId, userBolt, streamId, inputStream);
						break;
					case 2:
						Entry<GlobalStreamId, DataStream<Tuple>> secondInput = iterator.next();
						GlobalStreamId streamId2 = secondInput.getKey();
						DataStream<Tuple> inputStream2 = secondInput.getValue();
						outputStream = createOutput(boltId, userBolt, streamId, inputStream, streamId2, inputStream2);
						break;
					default:
						throw new UnsupportedOperationException("Don't know how to translate a bolt "
								+ boltId + " with " + numberOfInputs + " inputs.");
				}

				if (common.is_set_parallelism_hint()) {
					int dop = common.get_parallelism_hint();
					outputStream.setParallelism(dop);
				} else {
					common.set_parallelism_hint(1);
				}

			}
		}
	}

	private DataStream<Tuple> processInput(String boltId, IRichBolt userBolt,
										GlobalStreamId streamId, Grouping grouping,
										Map<String, DataStream<Tuple>> producer) {

		assert (userBolt != null);
		assert(boltId != null);
		assert(streamId != null);
		assert(grouping != null);
		assert(producer != null);

		final String producerId = streamId.get_componentId();
		final String inputStreamId = streamId.get_streamId();

		DataStream<Tuple> inputStream = producer.get(inputStreamId);

		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
		declarers.put(boltId, declarer);
		userBolt.declareOutputFields(declarer);
		this.outputStreams.put(boltId, declarer.outputStreams);

		// if producer was processed already
		if (grouping.is_set_shuffle()) {
			// Storm uses a round-robin shuffle strategy
			inputStream = inputStream.rebalance();
		} else if (grouping.is_set_fields()) {
			// global grouping is emulated in Storm via an empty fields grouping list
			final List<String> fields = grouping.get_fields();
			if (fields.size() > 0) {
				FlinkOutputFieldsDeclarer prodDeclarer = this.declarers.get(producerId);
				inputStream = inputStream.keyBy(prodDeclarer
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

		return inputStream;
	}

	private SingleOutputStreamOperator<?, ?> createOutput(String boltId, IRichBolt bolt, GlobalStreamId streamId, DataStream<Tuple> inputStream) {
		return createOutput(boltId, bolt, streamId, inputStream, null, null);
	}

	private SingleOutputStreamOperator<?, ?> createOutput(String boltId, IRichBolt bolt,
														GlobalStreamId streamId, DataStream<Tuple> inputStream,
														GlobalStreamId streamId2, DataStream<Tuple> inputStream2) {
		assert(boltId != null);
		assert(streamId != null);
		assert(inputStream != null);
		Preconditions.checkArgument((streamId2 == null) == (inputStream2 == null));

		String producerId = streamId.get_componentId();
		String inputStreamId = streamId.get_streamId();

		final HashMap<String, Fields> boltOutputs = this.outputStreams.get(boltId);

		final FlinkOutputFieldsDeclarer declarer = declarers.get(boltId);

		final SingleOutputStreamOperator<?, ?> outputStream;

		if (boltOutputs.size() < 2) { // single output stream or sink
			String outputStreamId;
			if (boltOutputs.size() == 1) {
				outputStreamId = (String) boltOutputs.keySet().toArray()[0];
			} else {
				outputStreamId = null;
			}

			final TypeInformation<Tuple> outType = declarer
					.getOutputType(outputStreamId);

			final SingleOutputStreamOperator<Tuple, ?> outStream;

			// only one input
			if (streamId2 == null) {
				BoltWrapper<Tuple, Tuple> boltWrapper = new BoltWrapper<>(
						bolt, boltId, producerId, inputStreamId,
						this.outputStreams.get(producerId).get(inputStreamId), null);
				boltWrapper.setStormTopology(stormTopology);


				outStream = inputStream.transform(boltId, outType, boltWrapper);

			} else {
				String producerId2 = streamId2.get_componentId();
				String inputStreamId2 = streamId2.get_streamId();

				final BoltWrapperTwoInput<Tuple, Tuple, Tuple> boltWrapper = new BoltWrapperTwoInput<>(
						bolt, boltId,
					inputStreamId, inputStreamId2, producerId, producerId2,
					this.outputStreams.get(producerId).get(inputStreamId),
						this.outputStreams.get(producerId2).get(inputStreamId2)
				);
				boltWrapper.setStormTopology(stormTopology);

				outStream = inputStream.connect(inputStream2).transform(boltId, outType, boltWrapper);
			}

			if (outType != null) {
				// only for non-sink nodes
				final HashMap<String, DataStream<Tuple>> op = new HashMap<>();
				op.put(outputStreamId, outStream);
				availableInputs.put(boltId, op);
			}
			outputStream = outStream;
		} else {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			final TypeInformation<SplitStreamType<Tuple>> outType = (TypeInformation) TypeExtractor
					.getForClass(SplitStreamType.class);


			final SingleOutputStreamOperator<SplitStreamType<Tuple>, ?> multiStream;

			// only one input
			if (streamId2 == null) {
				final BoltWrapper<Tuple, SplitStreamType<Tuple>> boltWrapperMultipleOutputs = new BoltWrapper<>(
					bolt, boltId, inputStreamId, producerId, this.outputStreams.get(producerId).get(inputStreamId),
					null
				);
				boltWrapperMultipleOutputs.setStormTopology(stormTopology);

				multiStream = inputStream.transform(boltId, outType, boltWrapperMultipleOutputs);
			} else {
				String producerId2 = streamId2.get_componentId();
				String inputStreamId2 = streamId2.get_streamId();

				final BoltWrapperTwoInput<Tuple, Tuple, SplitStreamType<Tuple>> boltWrapper = new BoltWrapperTwoInput<>(
						bolt, boltId,
					inputStreamId, inputStreamId2, producerId, producerId2,
					this.outputStreams.get(producerId).get(inputStreamId),
					this.outputStreams.get(producerId2).get(inputStreamId2)
				);
				boltWrapper.setStormTopology(stormTopology);

				multiStream = inputStream.connect(inputStream2).transform(boltId, outType, boltWrapper);
			}

			final SplitStream<SplitStreamType<Tuple>> splitStream = multiStream
					.split(new StormStreamSelector<Tuple>());

			final HashMap<String, DataStream<Tuple>> op = new HashMap<>();
			for (String outputStreamId : boltOutputs.keySet()) {
				op.put(outputStreamId,
						splitStream.select(outputStreamId).map(
								new SplitStreamMapper<Tuple>()));
				SingleOutputStreamOperator<Tuple, ?> outStream = splitStream
						.select(outputStreamId).map(new SplitStreamMapper<Tuple>());
				outStream.getTransformation().setOutputType(declarer.getOutputType(outputStreamId));
				op.put(outputStreamId, outStream);
			}
			availableInputs.put(boltId, op);
			outputStream = multiStream;
		}

		return outputStream;
	}

	// for internal testing purpose only
	public StormTopology getStormTopology() {
		return this.stormTopology;
	}
}
