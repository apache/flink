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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link OneInputTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to translate.
 * @param <OUT> The type of the elements that result from the provided {@code OneInputTransformation}.
 */
@Internal
public class OneInputTransformationTranslator<IN, OUT> extends SimpleTransformationTranslator<OUT, OneInputTransformation<IN, OUT>> {

	@Override
	public Collection<Integer> translateForBatchInternal(
			final OneInputTransformation<IN, OUT> transformation,
			final Context context) {
		Collection<Integer> ids = translateInternal(transformation, context);
		boolean isKeyed = transformation.getStateKeySelector() != null;
		if (isKeyed) {
			BatchExecutionUtils.applySortingInputs(transformation.getId(), context);
		}

		return ids;
	}

	@Override
	public Collection<Integer> translateForStreamingInternal(
			final OneInputTransformation<IN, OUT> transformation,
			final Context context) {
		return translateInternal(transformation, context);
	}

	private Collection<Integer> translateInternal(
			final OneInputTransformation<IN, OUT> transformation,
			final Context context) {
		checkNotNull(transformation);
		checkNotNull(context);

		final StreamGraph streamGraph = context.getStreamGraph();
		final String slotSharingGroup = context.getSlotSharingGroup();
		final int transformationId = transformation.getId();
		final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

		streamGraph.addOperator(
				transformationId,
				slotSharingGroup,
				transformation.getCoLocationGroupKey(),
				transformation.getOperatorFactory(),
				transformation.getInputType(),
				transformation.getOutputType(),
				transformation.getName());

		if (transformation.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = transformation.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(transformationId, transformation.getStateKeySelector(), keySerializer);
		}

		int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? transformation.getParallelism()
				: executionConfig.getParallelism();
		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

		final List<Transformation<?>> parentTransformations = transformation.getInputs();
		checkState(
				parentTransformations.size() == 1,
				"Expected exactly one input transformation but found " + parentTransformations.size());

		for (Integer inputId: context.getStreamNodeIds(parentTransformations.get(0))) {
			streamGraph.addEdge(inputId, transformationId, 0);
		}

		return Collections.singleton(transformationId);
	}
}
