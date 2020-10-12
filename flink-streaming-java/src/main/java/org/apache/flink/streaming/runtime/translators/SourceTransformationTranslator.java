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
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link SourceTransformation}.
 *
 * @param <OUT> The type of the elements that this source produces.
 */
@Internal
public class SourceTransformationTranslator<OUT>
		extends SimpleTransformationTranslator<OUT, SourceTransformation<OUT>> {

	@Override
	protected Collection<Integer> translateForBatchInternal(
			final SourceTransformation<OUT> transformation,
			final Context context) {
		return translateInternal(transformation, context);
	}

	@Override
	protected Collection<Integer> translateForStreamingInternal(
			final SourceTransformation<OUT> transformation,
			final Context context) {
		return translateInternal(transformation, context);
	}

	private Collection<Integer> translateInternal(
			final SourceTransformation<OUT> transformation,
			final Context context) {
		checkNotNull(transformation);
		checkNotNull(context);

		final StreamGraph streamGraph = context.getStreamGraph();
		final String slotSharingGroup = context.getSlotSharingGroup();
		final int transformationId = transformation.getId();
		final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

		streamGraph.addSource(
				transformationId,
				slotSharingGroup,
				transformation.getCoLocationGroupKey(),
				transformation.getOperatorFactory(),
				null,
				transformation.getOutputType(),
				"Source: " + transformation.getName());

		final int parallelism = transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
				? transformation.getParallelism()
				: executionConfig.getParallelism();

		streamGraph.setParallelism(transformationId, parallelism);
		streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());
		return Collections.singleton(transformationId);
	}
}
