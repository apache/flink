/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunctionV2;
import org.apache.flink.streaming.api.graph.StreamNode.ReadPriority;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SelectTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceV2Transformation;
import org.apache.flink.streaming.api.transformations.SplitTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder;

/**
 * A generator that generates a {@link StreamGraph} from a graph of
 * {@link StreamTransformation StreamTransformations}.
 *
 * <p>This traverses the tree of {@code StreamTransformations} starting from the sinks. At each
 * transformation we recursively transform the inputs, then create a node in the {@code StreamGraph}
 * and add edges from the input Nodes to our newly created node. The transformation methods
 * return the IDs of the nodes in the StreamGraph that represent the input transformation. Several
 * IDs can be returned to be able to deal with feedback transformations and unions.
 *
 * <p>Partitioning, split/select and union don't create actual nodes in the {@code StreamGraph}. For
 * these, we create a virtual node in the {@code StreamGraph} that holds the specific property, i.e.
 * partitioning, selector and so on. When an edge is created from a virtual node to a downstream
 * node the {@code StreamGraph} resolved the id of the original node and creates an edge
 * in the graph with the desired property. For example, if you have this graph:
 *
 * <pre>
 *     Map-1 -&gt; HashPartition-2 -&gt; Map-3
 * </pre>
 *
 * <p>where the numbers represent transformation IDs. We first recurse all the way down. {@code Map-1}
 * is transformed, i.e. we create a {@code StreamNode} with ID 1. Then we transform the
 * {@code HashPartition}, for this, we create virtual node of ID 4 that holds the property
 * {@code HashPartition}. This transformation returns the ID 4. Then we transform the {@code Map-3}.
 * We add the edge {@code 4 -> 3}. The {@code StreamGraph} resolved the actual node with ID 1 and
 * creates and edge {@code 1 -> 3} with the property HashPartition.
 */
@Internal
public class StreamGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphGenerator.class);

	public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
	public static final int UPPER_BOUND_MAX_PARALLELISM = KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

	// The StreamGraph that is being built, this is initialized at the beginning.
	private final StreamGraph streamGraph;

	private final Context context;

	// This is used to assign a unique ID to iteration source/sink
	protected static Integer iterationIdCounter = 0;
	public static int getNewIterationNodeId() {
		iterationIdCounter--;
		return iterationIdCounter;
	}

	// Keep track of which Transforms we have already transformed, this is necessary because
	// we have loops, i.e. feedback edges.
	private Map<StreamTransformation<?>, Collection<Integer>> alreadyTransformed;


	/**
	 * Private constructor. The generator should only be invoked using {@link #generate}.
	 */
	private StreamGraphGenerator(Context context) {
		this.context = context;
		this.streamGraph = new StreamGraph(context.getExecutionConfig(),
			context.getCheckpointConfig(),
			context.getDefaultParallelism(),
			context.getBufferTimeout(),
			DataPartitionerType.valueOf(context.getDefaultPartitioner()));
		this.streamGraph.setJobName(context.getJobName());
		this.streamGraph.getCustomConfiguration().setString(ScheduleMode.class.getName(), context.getScheduleMode().toString());
		this.streamGraph.setTimeCharacteristic(context.getTimeCharacteristic());
		this.streamGraph.setCachedFiles(context.getCacheFiles());
		this.streamGraph.setChaining(context.isChainingEnabled());
		this.streamGraph.setMultiHeadChainMode(context.isMultiHeadChainMode());
		this.streamGraph.setChainEagerlyEnabled(context.isChainEagerlyEnabled());
		this.streamGraph.setStateBackend(context.getStateBackend());
		this.streamGraph.getCustomConfiguration().addAll(context.getConfiguration());
		this.alreadyTransformed = new HashMap<>();

		this.streamGraph.addCustomConfiguration(context.getCustomConfiguration());
	}

	/**
	 * Generates a {@code StreamGraph} by traversing the graph of {@code StreamTransformations}
	 * starting from the given transformations.
	 *
	 * @param context The {@code StreamExecutionEnvironment} that is used to set some parameters of the
	 *            job
	 * @param transformations The transformations starting from which to transform the graph
	 *
	 * @return The generated {@code StreamGraph}
	 */
	public static StreamGraph generate(Context context, List<StreamTransformation<?>> transformations) {
		return new StreamGraphGenerator(context).generateInternal(transformations);
	}

	/**
	 * This starts the actual transformation, beginning from the sinks.
	 */
	private StreamGraph generateInternal(List<StreamTransformation<?>> transformations) {
		for (StreamTransformation<?> transformation: transformations) {
			transform(transformation);
		}

		// set default resources for operators
		boolean needToSetDefaultResources = false;
		if (context.getDefaultResources() == null || ResourceSpec.DEFAULT.equals(context.getDefaultResources())) {
			for (StreamNode node : streamGraph.getStreamNodes()) {
				ResourceSpec resources = node.getMinResources();
				if (resources != null && !ResourceSpec.DEFAULT.equals(resources)) {
					needToSetDefaultResources = true;
					break;
				}
			}
		} else {
			needToSetDefaultResources = true;
		}

		if (needToSetDefaultResources) {
			ResourceSpec defaultResource = context.getDefaultResources();
			if (defaultResource == null || ResourceSpec.DEFAULT.equals(defaultResource)) {
				defaultResource = context.getGlobalDefaultResources();
			}

			for (StreamNode node : streamGraph.getStreamNodes()) {
				ResourceSpec resources = node.getMinResources();
				if (resources == null || ResourceSpec.DEFAULT.equals(resources)) {
					node.setResources(defaultResource, defaultResource);
				}
			}
		}

		return streamGraph;
	}

	/**
	 * Transforms one {@code StreamTransformation}.
	 *
	 * <p>This checks whether we already transformed it and exits early in that case. If not it
	 * delegates to one of the transformation specific methods.
	 */
	private Collection<Integer> transform(StreamTransformation<?> transform) {

		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		LOG.debug("Transforming " + transform);

		if (transform.getMaxParallelism() <= 0) {

			// if the max parallelism hasn't been set, then first use the job wide max parallelism
			// from theExecutionConfig.
			int globalMaxParallelismFromConfig = context.getExecutionConfig().getMaxParallelism();
			if (globalMaxParallelismFromConfig > 0) {
				transform.setMaxParallelism(globalMaxParallelismFromConfig);
			}
		}

		// call at least once to trigger exceptions about MissingTypeInfo
		transform.getOutputType();

		Collection<Integer> transformedIds;
		if (transform instanceof OneInputTransformation<?, ?>) {
			transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
		} else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
			transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>) transform);
		} else if (transform instanceof SourceTransformation<?>) {
			transformedIds = transformSource((SourceTransformation<?>) transform);
		} else if (transform instanceof SourceV2Transformation<?>) {
			transformedIds = transformSourceV2((SourceV2Transformation<?>) transform);
		} else if (transform instanceof SinkTransformation<?>) {
			transformedIds = transformSink((SinkTransformation<?>) transform);
		} else if (transform instanceof UnionTransformation<?>) {
			transformedIds = transformUnion((UnionTransformation<?>) transform);
		} else if (transform instanceof SplitTransformation<?>) {
			transformedIds = transformSplit((SplitTransformation<?>) transform);
		} else if (transform instanceof SelectTransformation<?>) {
			transformedIds = transformSelect((SelectTransformation<?>) transform);
		} else if (transform instanceof FeedbackTransformation<?>) {
			transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
		} else if (transform instanceof CoFeedbackTransformation<?>) {
			transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
		} else if (transform instanceof PartitionTransformation<?>) {
			transformedIds = transformPartition((PartitionTransformation<?>) transform);
		} else if (transform instanceof SideOutputTransformation<?>) {
			transformedIds = transformSideOutput((SideOutputTransformation<?>) transform);
		} else {
			throw new IllegalStateException("Unknown transformation: " + transform);
		}

		// need this check because the iterate transformation adds itself before
		// transforming the feedback edges
		if (!alreadyTransformed.containsKey(transform)) {
			alreadyTransformed.put(transform, transformedIds);
		}

		if (transform.getBufferTimeout() >= 0) {
			streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
		}
		if (transform.getUid() != null) {
			streamGraph.setTransformationUID(transform.getId(), transform.getUid());
		}
		if (transform.getUserProvidedNodeHash() != null) {
			streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
		}

		if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
			streamGraph.setResources(transform.getId(), transform.getMinResources(), transform.getPreferredResources());
		}

		if (transform.getCustomConfiguration().keySet().size() > 0) {
			streamGraph.addCustomConfiguration(transform.getId(), transform.getCustomConfiguration());
		}

		return transformedIds;
	}

	/**
	 * Transforms a {@code UnionTransformation}.
	 *
	 * <p>This is easy, we only have to transform the inputs and return all the IDs in a list so
	 * that downstream operations can connect to all upstream nodes.
	 */
	private <T> Collection<Integer> transformUnion(UnionTransformation<T> union) {
		List<StreamTransformation<T>> inputs = union.getInputs();
		List<Integer> resultIds = new ArrayList<>();

		for (StreamTransformation<T> input: inputs) {
			resultIds.addAll(transform(input));
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code PartitionTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} that holds the partition
	 * property. @see StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformPartition(PartitionTransformation<T> partition) {
		StreamTransformation<T> input = partition.getInput();
		List<Integer> resultIds = new ArrayList<>();

		Collection<Integer> transformedIds = transform(input);
		for (Integer transformedId: transformedIds) {
			int virtualId = StreamTransformation.getNewNodeId();
			streamGraph.addVirtualPartitionNode(transformedId, virtualId, partition.getPartitioner(), partition.getDataExchangeMode());
			resultIds.add(virtualId);
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code SplitTransformation}.
	 *
	 * <p>We add the output selector to previously transformed nodes.
	 */
	private <T> Collection<Integer> transformSplit(SplitTransformation<T> split) {

		StreamTransformation<T> input = split.getInput();
		Collection<Integer> resultIds = transform(input);

		// the recursive transform call might have transformed this already
		if (alreadyTransformed.containsKey(split)) {
			return alreadyTransformed.get(split);
		}

		for (int inputId : resultIds) {
			streamGraph.addOutputSelector(inputId, split.getOutputSelector());
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code SelectTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} holds the selected names.
	 *
	 * @see org.apache.flink.streaming.api.graph.StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformSelect(SelectTransformation<T> select) {
		StreamTransformation<T> input = select.getInput();
		Collection<Integer> resultIds = transform(input);

		// the recursive transform might have already transformed this
		if (alreadyTransformed.containsKey(select)) {
			return alreadyTransformed.get(select);
		}

		List<Integer> virtualResultIds = new ArrayList<>();

		for (int inputId : resultIds) {
			int virtualId = StreamTransformation.getNewNodeId();
			streamGraph.addVirtualSelectNode(inputId, virtualId, select.getSelectedNames());
			virtualResultIds.add(virtualId);
		}
		return virtualResultIds;
	}

	/**
	 * Transforms a {@code SideOutputTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} that holds the side-output
	 * {@link org.apache.flink.util.OutputTag}.
	 *
	 * @see org.apache.flink.streaming.api.graph.StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformSideOutput(SideOutputTransformation<T> sideOutput) {
		StreamTransformation<?> input = sideOutput.getInput();
		Collection<Integer> resultIds = transform(input);

		// the recursive transform might have already transformed this
		if (alreadyTransformed.containsKey(sideOutput)) {
			return alreadyTransformed.get(sideOutput);
		}

		List<Integer> virtualResultIds = new ArrayList<>();

		for (int inputId : resultIds) {
			int virtualId = StreamTransformation.getNewNodeId();
			streamGraph.addVirtualSideOutputNode(inputId, virtualId, sideOutput.getOutputTag(), sideOutput.getDamBehavior());
			virtualResultIds.add(virtualId);
		}
		return virtualResultIds;
	}

	/**
	 * Transforms a {@code FeedbackTransformation}.
	 *
	 * <p>This will recursively transform the input and the feedback edges. We return the
	 * concatenation of the input IDs and the feedback IDs so that downstream operations can be
	 * wired to both.
	 *
	 * <p>This is responsible for creating the IterationSource and IterationSink which are used to
	 * feed back the elements.
	 */
	private <T> Collection<Integer> transformFeedback(FeedbackTransformation<T> iterate) {

		if (iterate.getFeedbackEdges().size() <= 0) {
			throw new IllegalStateException("Iteration " + iterate + " does not have any feedback edges.");
		}

		StreamTransformation<T> input = iterate.getInput();
		List<Integer> resultIds = new ArrayList<>();

		// first transform the input stream(s) and store the result IDs
		Collection<Integer> inputIds = transform(input);
		resultIds.addAll(inputIds);

		// the recursive transform might have already transformed this
		if (alreadyTransformed.containsKey(iterate)) {
			return alreadyTransformed.get(iterate);
		}

		// create the fake iteration source/sink pair
		Tuple2<StreamNode, StreamNode> itSourceAndSink = streamGraph.createIterationSourceAndSink(
			iterate.getId(),
			getNewIterationNodeId(),
			getNewIterationNodeId(),
			iterate.getWaitTime(),
			iterate.getParallelism(),
			iterate.getMaxParallelism(),
			iterate.getMinResources(),
			iterate.getPreferredResources());

		StreamNode itSource = itSourceAndSink.f0;
		StreamNode itSink = itSourceAndSink.f1;

		// We set the proper serializers for the sink/source
		streamGraph.setSerializers(itSource.getId(), null, null, iterate.getOutputType().createSerializer(context.getExecutionConfig()));
		streamGraph.setSerializers(itSink.getId(), iterate.getOutputType().createSerializer(context.getExecutionConfig()), null, null);

		// also add the feedback source ID to the result IDs, so that downstream operators will
		// add both as input
		resultIds.add(itSource.getId());

		// at the iterate to the already-seen-set with the result IDs, so that we can transform
		// the feedback edges and let them stop when encountering the iterate node
		alreadyTransformed.put(iterate, resultIds);

		// so that we can determine the slot sharing group from all feedback edges
		List<Integer> allFeedbackIds = new ArrayList<>();

		for (StreamTransformation<T> feedbackEdge : iterate.getFeedbackEdges()) {
			Collection<Integer> feedbackIds = transform(feedbackEdge);
			allFeedbackIds.addAll(feedbackIds);
			for (Integer feedbackId: feedbackIds) {
				streamGraph.addEdge(feedbackId,
						itSink.getId(),
						0
				);
			}
		}

		String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds, true);

		itSink.setSlotSharingGroup(slotSharingGroup);
		itSource.setSlotSharingGroup(slotSharingGroup);

		return resultIds;
	}

	/**
	 * Transforms a {@code CoFeedbackTransformation}.
	 *
	 * <p>This will only transform feedback edges, the result of this transform will be wired
	 * to the second input of a Co-Transform. The original input is wired directly to the first
	 * input of the downstream Co-Transform.
	 *
	 * <p>This is responsible for creating the IterationSource and IterationSink which
	 * are used to feed back the elements.
	 */
	private <F> Collection<Integer> transformCoFeedback(CoFeedbackTransformation<F> coIterate) {

		// For Co-Iteration we don't need to transform the input and wire the input to the
		// head operator by returning the input IDs, the input is directly wired to the left
		// input of the co-operation. This transform only needs to return the ids of the feedback
		// edges, since they need to be wired to the second input of the co-operation.

		// create the fake iteration source/sink pair
		Tuple2<StreamNode, StreamNode> itSourceAndSink = streamGraph.createIterationSourceAndSink(
				coIterate.getId(),
				getNewIterationNodeId(),
				getNewIterationNodeId(),
				coIterate.getWaitTime(),
				coIterate.getParallelism(),
				coIterate.getMaxParallelism(),
				coIterate.getMinResources(),
				coIterate.getPreferredResources());

		StreamNode itSource = itSourceAndSink.f0;
		StreamNode itSink = itSourceAndSink.f1;

		// We set the proper serializers for the sink/source
		streamGraph.setSerializers(itSource.getId(), null, null, coIterate.getOutputType().createSerializer(context.getExecutionConfig()));
		streamGraph.setSerializers(itSink.getId(), coIterate.getOutputType().createSerializer(context.getExecutionConfig()), null, null);

		Collection<Integer> resultIds = Collections.singleton(itSource.getId());

		// at the iterate to the already-seen-set with the result IDs, so that we can transform
		// the feedback edges and let them stop when encountering the iterate node
		alreadyTransformed.put(coIterate, resultIds);

		// so that we can determine the slot sharing group from all feedback edges
		List<Integer> allFeedbackIds = new ArrayList<>();

		for (StreamTransformation<F> feedbackEdge : coIterate.getFeedbackEdges()) {
			Collection<Integer> feedbackIds = transform(feedbackEdge);
			allFeedbackIds.addAll(feedbackIds);
			for (Integer feedbackId: feedbackIds) {
				streamGraph.addEdge(feedbackId,
						itSink.getId(),
						0
				);
			}
		}

		String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds, true);

		itSink.setSlotSharingGroup(slotSharingGroup);
		itSource.setSlotSharingGroup(slotSharingGroup);

		return Collections.singleton(itSource.getId());
	}

	/**
	 * Transforms a {@code SourceTransformation}.
	 */
	private <T> Collection<Integer> transformSource(SourceTransformation<T> source) {
		String slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(), new ArrayList<Integer>(), false);
		streamGraph.addSource(source.getId(),
				slotSharingGroup,
				source.getOperator(),
				null,
				source.getOutputType(),
				"Source: " + source.getName());
		if (source.getOperator().getUserFunction() instanceof InputFormatSourceFunction) {
			InputFormatSourceFunction<T> fs = (InputFormatSourceFunction<T>) source.getOperator().getUserFunction();
			streamGraph.setInputFormat(source.getId(), fs.getFormat());
		}
		streamGraph.setParallelism(source.getId(), source.getParallelism());
		streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
		return Collections.singleton(source.getId());
	}

	/**
	 * Transforms a {@code SourceTransformation}.
	 */
	private <T> Collection<Integer> transformSourceV2(SourceV2Transformation<T> source) {
		String slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(), new ArrayList<Integer>(), false);
		streamGraph.addSource(source.getId(),
			slotSharingGroup,
			source.getOperator(),
			null,
			source.getOutputType(),
			"Source: " + source.getName());
		if (source.getOperator().getUserFunction() instanceof InputFormatSourceFunctionV2) {
			InputFormatSourceFunctionV2<T> fs = (InputFormatSourceFunctionV2<T>) source.getOperator().getUserFunction();
			streamGraph.setInputFormat(source.getId(), fs.getFormat());
		}
		streamGraph.setParallelism(source.getId(), source.getParallelism());
		streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
		return Collections.singleton(source.getId());
	}

	/**
	 * Transforms a {@code SourceTransformation}.
	 */
	private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {

		Collection<Integer> inputIds = transform(sink.getInput());

		String slotSharingGroup = determineSlotSharingGroup(sink.getSlotSharingGroup(), inputIds, false);

		streamGraph.addSink(sink.getId(),
				slotSharingGroup,
				sink.getOperator(),
				sink.getInput().getOutputType(),
				null,
				"Sink: " + sink.getName());

		if (sink.getOperator().getUserFunction() instanceof OutputFormatSinkFunction) {
			OutputFormatSinkFunction<T> fs = (OutputFormatSinkFunction<T>) sink.getOperator().getUserFunction();
			streamGraph.setOutputFormat(sink.getId(), fs.getFormat());
		}
		streamGraph.setParallelism(sink.getId(), sink.getParallelism());
		streamGraph.setMaxParallelism(sink.getId(), sink.getMaxParallelism());

		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId,
					sink.getId(),
					0
			);
		}

		if (sink.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = sink.getStateKeyType().createSerializer(context.getExecutionConfig());
			streamGraph.setOneInputStateKey(sink.getId(), sink.getStateKeySelector(), keySerializer);
		}

		return Collections.emptyList();
	}

	/**
	 * Transforms a {@code OneInputTransformation}.
	 *
	 * <p>This recursively transforms the inputs, creates a new {@code StreamNode} in the graph and
	 * wired the inputs to this new node.
	 */
	private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {

		Collection<Integer> inputIds = transform(transform.getInput());

		// the recursive call might have already transformed this
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), inputIds, false);

		streamGraph.addOperator(transform.getId(),
				slotSharingGroup,
				transform.getOperator(),
				transform.getInputType(),
				transform.getOutputType(),
				transform.getName());

		if (transform.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(context.getExecutionConfig());
			streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
		}

		streamGraph.setParallelism(transform.getId(), transform.getParallelism());
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());
		streamGraph.setMainOutputDamBehavior(transform.getId(), transform.getDamBehavior());

		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId, transform.getId(), 0);
		}

		return Collections.singleton(transform.getId());
	}

	/**
	 * Transforms a {@code TwoInputTransformation}.
	 *
	 * <p>This recursively transforms the inputs, creates a new {@code StreamNode} in the graph and
	 * wired the inputs to this new node.
	 */
	private <IN1, IN2, OUT> Collection<Integer> transformTwoInputTransform(TwoInputTransformation<IN1, IN2, OUT> transform) {

		Collection<Integer> inputIds1 = transform(transform.getInput1());
		Collection<Integer> inputIds2 = transform(transform.getInput2());

		// the recursive call might have already transformed this
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		List<Integer> allInputIds = new ArrayList<>();
		allInputIds.addAll(inputIds1);
		allInputIds.addAll(inputIds2);

		String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), allInputIds, false);

		streamGraph.addCoOperator(
				transform.getId(),
				slotSharingGroup,
				transform.getOperator(),
				transform.getInputType1(),
				transform.getInputType2(),
				transform.getOutputType(),
				transform.getName());

		if (transform.getStateKeySelector1() != null || transform.getStateKeySelector2() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(context.getExecutionConfig());
			streamGraph.setTwoInputStateKey(transform.getId(), transform.getStateKeySelector1(), transform.getStateKeySelector2(), keySerializer);
		}

		streamGraph.setParallelism(transform.getId(), transform.getParallelism());
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());
		streamGraph.setMainOutputDamBehavior(transform.getId(), transform.getDamBehavior());

		ReadPriority readPriority1 = null, readPriority2 = null;
		ReadOrder readOrderHint = transform.getReadOrderHint();
		if (ReadOrder.INPUT1_FIRST.equals(readOrderHint)) {
			readPriority1 = ReadPriority.HIGHER;
			readPriority2 = ReadPriority.LOWER;
		} else if (ReadOrder.INPUT2_FIRST.equals(readOrderHint)) {
			readPriority1 = ReadPriority.LOWER;
			readPriority2 = ReadPriority.HIGHER;
		} else if (ReadOrder.SPECIAL_ORDER.equals(readOrderHint)) {
			readPriority1 = ReadPriority.DYNAMIC;
			readPriority2 = ReadPriority.DYNAMIC;
		}

		Integer vertexID = transform.getId();
		for (Integer inputId: inputIds1) {
			StreamEdge inEdge = streamGraph.addEdge(inputId,
					transform.getId(),
					1
			);

			if (readPriority1 != null) {
				streamGraph.setReadPriorityHint(vertexID, inEdge, readPriority1);
			}
		}

		for (Integer inputId: inputIds2) {
			StreamEdge inEdge = streamGraph.addEdge(inputId,
					transform.getId(),
					2
			);

			if (readPriority2 != null) {
				streamGraph.setReadPriorityHint(vertexID, inEdge, readPriority2);
			}
		}

		return Collections.singleton(transform.getId());
	}

	/**
	 * Determines the slot sharing group for an operation based on the slot sharing group set by
	 * the user and the slot sharing groups of the inputs.
	 *
	 * <p>If the user specifies a group name, this is taken as is. If nothing is specified and
	 * the input operations all have the same group name then this name is taken. Otherwise the
	 * default group is chosen.
	 *
	 * @param specifiedGroup The group specified by the user.
	 * @param inputIds The IDs of the input operations.
	 */
	private String determineSlotSharingGroup(String specifiedGroup, Collection<Integer> inputIds, boolean isSlotSharingForced) {
		if (!context.isSlotSharingEnabled() && !isSlotSharingForced) {
			return null;
		}

		if (specifiedGroup != null) {
			return specifiedGroup;
		} else {
			String inputGroup = null;
			for (int id: inputIds) {
				String inputGroupCandidate = streamGraph.getSlotSharingGroup(id);
				if (inputGroup == null) {
					inputGroup = inputGroupCandidate;
				} else if (!inputGroup.equals(inputGroupCandidate)) {
					return "default";
				}
			}
			return inputGroup == null ? "default" : inputGroup;
		}
	}

	/** An container used for keep properties of StreamGraph. **/
	public static class Context {
		private ExecutionConfig executionConfig;
		private CheckpointConfig checkpointConfig;
		private Configuration customConfiguration;
		private TimeCharacteristic timeCharacteristic;
		private StateBackend stateBackend;
		private boolean chainingEnabled;
		private boolean isMultiHeadChainMode;
		private boolean isSlotSharingEnabled;
		private boolean chainEagerlyEnabled;
		private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;
		private List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile = new ArrayList<>();
		private ScheduleMode scheduleMode;
		private long bufferTimeout;
		private Configuration configuration = new Configuration();

		private int defaultParallelism;
		private String defaultPartitioner;
		private ResourceSpec defaultResources;
		private ResourceSpec globalDefaultResources;

		public static Context buildStreamProperties(StreamExecutionEnvironment env) {
			Context context = new Context();

			context.setExecutionConfig(env.getConfig());
			context.setCheckpointConfig(env.getCheckpointConfig());
			context.setCustomConfiguration(env.getCustomConfiguration());
			context.setTimeCharacteristic(env.getStreamTimeCharacteristic());
			context.setStateBackend(env.getStateBackend());
			context.setChainingEnabled(env.isChainingEnabled());
			context.setCacheFiles(env.getCachedFiles());
			context.setBufferTimeout(env.getBufferTimeout());
			context.setDefaultParallelism(env.getParallelism());
			context.setMultiHeadChainMode(env.isMultiHeadChainMode());
			context.setSlotSharingEnabled(env.isSlotSharingEnabled());

			// For infinite stream job, by default schedule tasks in eager mode
			context.setScheduleMode(ScheduleMode.EAGER);

			Configuration globalConf = GlobalConfiguration.loadConfiguration();
			context.setDefaultPartitioner(globalConf.getString(CoreOptions.DEFAULT_PARTITIONER));

			if (globalConf.contains(CoreOptions.CHAIN_EAGERLY_ENABLED)) {
				context.setChainEagerlyEnabled(globalConf.getBoolean(CoreOptions.CHAIN_EAGERLY_ENABLED));
			} else {
				context.setChainEagerlyEnabled(false);
			}

			context.setDefaultResources(env.getDefaultResources());
			context.setGlobalDefaultResources(new ResourceSpec.Builder()
					.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
					.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
					.build());

			return context;
		}

		public static Context buildBatchProperties(StreamExecutionEnvironment env) {
			Context context = new Context();
			try {
				// we need to update some value in executionConfig.
				ExecutionConfig executionConfig = InstantiationUtil.clone(env.getConfig());
				executionConfig.enableObjectReuse();
				context.setExecutionConfig(executionConfig);
				executionConfig.setLatencyTrackingInterval(-1L);
				CheckpointConfig checkpointConfig = new CheckpointConfig();
				context.setCheckpointConfig(checkpointConfig);
				context.setCustomConfiguration(env.getCustomConfiguration());
				context.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
				context.setChainingEnabled(true);
				context.setCacheFiles(env.getCachedFiles());
				context.setBufferTimeout(-1L);
				context.setMultiHeadChainMode(true);
				context.setSlotSharingEnabled(env.isSlotSharingEnabled());

				// For finite stream job, by default schedule tasks in lazily from sources mode
				context.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

				Configuration globalConf = GlobalConfiguration.loadConfiguration();
				context.setDefaultPartitioner(globalConf.getString(CoreOptions.DEFAULT_PARTITIONER));

				if (globalConf.contains(CoreOptions.CHAIN_EAGERLY_ENABLED)) {
					context.setChainEagerlyEnabled(globalConf.getBoolean(CoreOptions.CHAIN_EAGERLY_ENABLED));
				} else {
					context.setChainEagerlyEnabled(false);
				}

				context.setDefaultResources(env.getDefaultResources());
				context.setGlobalDefaultResources(new ResourceSpec.Builder()
						.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
						.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
						.build());

				return context;
			} catch (IOException | ClassNotFoundException e) {
				throw new FlinkRuntimeException("This exception could not happen.", e);
			}
		}

		public void setExecutionConfig(ExecutionConfig executionConfig) {
			this.executionConfig = executionConfig;
		}

		public void setCheckpointConfig(CheckpointConfig checkpointConfig) {
			this.checkpointConfig = checkpointConfig;
		}

		public void setCustomConfiguration(Configuration customConfiguration) {
			this.customConfiguration = customConfiguration;
		}

		public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
			this.timeCharacteristic = timeCharacteristic;
		}

		public void setStateBackend(StateBackend stateBackend) {
			this.stateBackend = stateBackend;
		}

		public void setChainingEnabled(boolean chainingEnabled) {
			this.chainingEnabled = chainingEnabled;
		}

		public ExecutionConfig getExecutionConfig() {
			return executionConfig;
		}

		public CheckpointConfig getCheckpointConfig() {
			return checkpointConfig;
		}

		public Configuration getCustomConfiguration() {
			return customConfiguration;
		}

		public TimeCharacteristic getTimeCharacteristic() {
			return timeCharacteristic;
		}

		public StateBackend getStateBackend() {
			return stateBackend;
		}

		public boolean isChainingEnabled() {
			return chainingEnabled;
		}

		public String getJobName() {
			return jobName;
		}

		public void setJobName(String jobName) {
			this.jobName = jobName;
		}

		public void setCacheFiles(List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile) {
			this.cacheFile = cacheFile;
		}

		public List<Tuple2<String, DistributedCache.DistributedCacheEntry>> getCacheFiles() {
			return cacheFile;
		}

		public ScheduleMode getScheduleMode() {
			return scheduleMode;
		}

		public void setScheduleMode(ScheduleMode scheduleMode) {
			this.scheduleMode = scheduleMode;
		}

		public long getBufferTimeout() {
			return bufferTimeout;
		}

		public void setBufferTimeout(long bufferTimeout) {
			this.bufferTimeout = bufferTimeout;
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		public void setConfiguration(Configuration configuration) {
			this.configuration = configuration;
		}

		public int getDefaultParallelism() {
			return defaultParallelism;
		}

		public void setDefaultParallelism(int defaultParallelism) {
			this.defaultParallelism = defaultParallelism;
		}

		public String getDefaultPartitioner() {
			return defaultPartitioner;
		}

		public void setDefaultPartitioner(String defaultPartitioner) {
			this.defaultPartitioner = defaultPartitioner;
		}

		public boolean isMultiHeadChainMode() {
			return isMultiHeadChainMode;
		}

		public void setMultiHeadChainMode(boolean multiHeadChainMode) {
			isMultiHeadChainMode = multiHeadChainMode;
		}

		public boolean isSlotSharingEnabled() {
			return isSlotSharingEnabled;
		}

		public void setSlotSharingEnabled(boolean slotSharingEnabled) {
			isSlotSharingEnabled = slotSharingEnabled;
		}

		public boolean isChainEagerlyEnabled() {
			return chainEagerlyEnabled;
		}

		public void setChainEagerlyEnabled(boolean chainEagerlyEnabled) {
			this.chainEagerlyEnabled = chainEagerlyEnabled;
		}

		public ResourceSpec getDefaultResources() {
			return defaultResources;
		}

		public void setDefaultResources(ResourceSpec resources) {
			this.defaultResources = resources;
		}

		public ResourceSpec getGlobalDefaultResources() {
			return globalDefaultResources;
		}

		public void setGlobalDefaultResources(ResourceSpec resources) {
			this.globalDefaultResources = resources;
		}
	}
}
