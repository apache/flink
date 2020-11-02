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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.BatchGroupedReduceOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;

import java.util.Collection;

/**
 * A {@link TransformationTranslator} for the {@link ReduceTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to translate.
 */
public class ReduceTransformationTranslator<IN, KEY>
		extends AbstractOneInputTransformationTranslator<IN, IN, ReduceTransformation<IN, KEY>> {
	@Override
	public Collection<Integer> translateForBatchInternal(
			final ReduceTransformation<IN, KEY> transformation,
			final Context context) {
		BatchGroupedReduceOperator<IN, KEY> groupedReduce = new BatchGroupedReduceOperator<>(
			transformation.getReducer(),
			transformation
				.getInputType()
				.createSerializer(context.getStreamGraph().getExecutionConfig())
		);
		SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(groupedReduce);
		operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
		Collection<Integer> ids = translateInternal(
			transformation,
			operatorFactory,
			transformation.getInputType(),
			transformation.getKeySelector(),
			transformation.getKeyTypeInfo(),
			context);
		BatchExecutionUtils.applySortingInputs(transformation.getId(), context);

		return ids;
	}

	@Override
	public Collection<Integer> translateForStreamingInternal(
			final ReduceTransformation<IN, KEY> transformation,
			final Context context) {
		StreamGroupedReduceOperator<IN> groupedReduce = new StreamGroupedReduceOperator<>(
			transformation.getReducer(),
			transformation
				.getInputType()
				.createSerializer(context.getStreamGraph().getExecutionConfig())
		);

		SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(groupedReduce);
		operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
		return translateInternal(
			transformation,
			operatorFactory,
			transformation.getInputType(),
			transformation.getKeySelector(),
			transformation.getKeyTypeInfo(),
			context);
	}

}
