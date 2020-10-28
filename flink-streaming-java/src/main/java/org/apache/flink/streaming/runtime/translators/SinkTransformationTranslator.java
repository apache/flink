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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.BatchCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.BatchGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommittableTypeInformation;
import org.apache.flink.streaming.runtime.operators.sink.GlobalStreamingCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatefulWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatelessWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterOperatorFactory;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link SinkTransformation}.
 */
@Internal
public class SinkTransformationTranslator<InputT, CommT, WriterStateT, GlobalCommT> implements
		TransformationTranslator<Object, SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT>> {

	protected static final Logger LOG = LoggerFactory.getLogger(SinkTransformationTranslator.class);

	@Override
	public Collection<Integer> translateForBatch(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);
		final int parallelism = getParallelism(transformation, context);

		int writerId = addWriter(
				transformation,
				parallelism,
				context);
		int committerId = addCommitter(
				writerId,
				transformation,
				new BatchCommitterOperatorFactory<>(transformation.getSink()),
				1,
				1,
				context);
		addGlobalCommitter(
				committerId >= 0 ? committerId : writerId,
				transformation,
				new BatchGlobalCommitterOperatorFactory<>(transformation.getSink()),
				context);
		return Collections.emptyList();
	}

	@Override
	public Collection<Integer> translateForStreaming(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> transformation,
			Context context) {

		StreamGraphUtils.validateTransformationUid(context.getStreamGraph(), transformation);

		final int parallelism = getParallelism(transformation, context);

		int writerId = addWriter(
				transformation,
				parallelism,
				context);
		int committerId = addCommitter(
				writerId,
				transformation,
				new StreamingCommitterOperatorFactory<>(transformation.getSink()),
				parallelism,
				transformation.getMaxParallelism(),
				context);
		addGlobalCommitter(
				committerId >= 0 ? committerId : writerId,
				transformation,
				new GlobalStreamingCommitterOperatorFactory<>(transformation.getSink()),
				context);

		return Collections.emptyList();
	}

	/**
	 * Add a sink writer node to the stream graph.
	 *
	 * @param sinkTransformation The transformation that the writer belongs to
	 * @param parallelism The parallelism of the writer
	 *
	 * @return The stream node id of the writer
	 */
	private int addWriter(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			int parallelism,
			Context context) {
		final boolean hasState = sinkTransformation
				.getSink()
				.getWriterStateSerializer()
				.isPresent();
		checkState(sinkTransformation.getInputs().size() == 1);
		@SuppressWarnings("unchecked")
		final Transformation<InputT> input = (Transformation<InputT>) sinkTransformation
				.getInputs()
				.get(0);
		final TypeInformation<InputT> inputTypeInfo = input.getOutputType();

		final StreamOperatorFactory<CommT> writer =
				hasState ? new StatefulWriterOperatorFactory<>(sinkTransformation.getSink()) : new StatelessWriterOperatorFactory<>(
						sinkTransformation.getSink());

		final ChainingStrategy chainingStrategy = sinkTransformation.getChainingStrategy();

		if (chainingStrategy != null) {
			writer.setChainingStrategy(chainingStrategy);
		}

		return addOperatorToStreamGraph(
				writer,
				input.getId(),
				inputTypeInfo,
				extractCommittableTypeInformation(sinkTransformation.getSink()),
				"Sink Writer:",
				parallelism,
				sinkTransformation.getMaxParallelism(),
				sinkTransformation,
				context);
	}

	/**
	 * Try to add a sink committer to the stream graph.
	 *
	 * @param inputId The committer's input stream node id
	 * @param sinkTransformation The transformation that the committer belongs to
	 * @param committerFactory The committer operator's factory
	 * @param parallelism The parallelism of the committer
	 *
	 * @return The stream node id of the committer or -1 if the sink topology does not include a committer.
	 */
	private int addCommitter(
			int inputId,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			OneInputStreamOperatorFactory<CommT, CommT> committerFactory,
			int parallelism,
			int maxParallelism,
			Context context) {

		if (!sinkTransformation.getSink().createCommitter().isPresent()) {
			return -1;
		}

		final CommittableTypeInformation<CommT> committableTypeInfo = extractCommittableTypeInformation(
				sinkTransformation.getSink());
		checkNotNull(committableTypeInfo);

		return addOperatorToStreamGraph(
				committerFactory, inputId,
				committableTypeInfo,
				committableTypeInfo,
				"Sink Committer:",
				parallelism,
				maxParallelism,
				sinkTransformation,
				context);
	}

	/**
	 * Try to add a sink global committer to the stream graph.
	 *
	 * @param inputId The global committer's input stream node id.
	 * @param sinkTransformation The transformation that the global committer belongs to
	 * @param globalCommitterFactory The global committer factory
	 */
	private void addGlobalCommitter(
			int inputId,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			OneInputStreamOperatorFactory<CommT, GlobalCommT> globalCommitterFactory,
			Context context) {

		if (!sinkTransformation.getSink().createGlobalCommitter().isPresent()) {
			return;
		}

		addOperatorToStreamGraph(
				globalCommitterFactory, inputId,
				checkNotNull(extractCommittableTypeInformation(sinkTransformation.getSink())),
				null,
				"Sink Global Committer:",
				1,
				1,
				sinkTransformation,
				context);
	}

	private int getParallelism(
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			Context context) {
		return sinkTransformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? sinkTransformation.getParallelism()
				: context.getStreamGraph().getExecutionConfig().getParallelism();
	}

	/**
	 * Add a operator to the {@link StreamGraph}.
	 *
	 * @param operatorFactory The operator factory
	 * @param inputId The upstream stream node id of the operator
	 * @param inTypeInfo The input type information of the operator
	 * @param outTypInfo The output type information of the operator
	 * @param prefix The prefix of the name and uid of the operator
	 * @param parallelism The parallelism of the operator
	 * @param maxParallelism The max parallelism of the operator
	 * @param sinkTransformation The sink transformation which the operator belongs to
	 *
	 * @return The stream node id of the operator
	 */
	private <IN, OUT> int addOperatorToStreamGraph(
			StreamOperatorFactory<OUT> operatorFactory, int inputId,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypInfo,
			String prefix,
			int parallelism,
			int maxParallelism,
			SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> sinkTransformation,
			Context context) {
		final StreamGraph streamGraph = context.getStreamGraph();
		final String slotSharingGroup = context.getSlotSharingGroup();
		final int transformationId = Transformation.getNewNodeId();

		streamGraph.addOperator(
				transformationId,
				slotSharingGroup,
				sinkTransformation.getCoLocationGroupKey(),
				operatorFactory,
				inTypeInfo,
				outTypInfo,
				String.format("%s %s", prefix, sinkTransformation.getName()));

		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, maxParallelism);

		StreamGraphUtils.configureBufferTimeout(
				streamGraph,
				transformationId,
				sinkTransformation,
				context.getDefaultBufferTimeout());
		if (sinkTransformation.getUid() != null) {
			streamGraph.setTransformationUID(
					transformationId,
					String.format("%s %s", prefix, sinkTransformation.getUid()));
		}
		streamGraph.addEdge(inputId, transformationId, 0);

		return transformationId;
	}

	private CommittableTypeInformation<CommT> extractCommittableTypeInformation(Sink<InputT, CommT, WriterStateT, GlobalCommT> sink) {
		if (sink.getCommittableSerializer().isPresent()) {
			final Type committableType = TypeExtractor.getParameterType(
					Sink.class,
					sink.getClass(),
					1);
			LOG.debug("Extracted committable type [{}] from sink [{}].", committableType.toString(), sink.getClass().getCanonicalName());
			return new CommittableTypeInformation<>(
					typeToClass(committableType),
					() -> sink.getCommittableSerializer().get());
		} else {
			return null;
		}
	}
}
