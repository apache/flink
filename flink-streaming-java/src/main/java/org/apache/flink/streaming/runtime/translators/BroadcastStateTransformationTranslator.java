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
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.BroadcastStateTransformation;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link BroadcastStateTransformation}.
 *
 * @param <IN1> The type of the elements in the non-broadcasted input of the {@link BroadcastStateTransformation}.
 * @param <IN2> The type of the elements in the broadcasted input of the {@link BroadcastStateTransformation}.
 * @param <OUT> The type of the elements that result from the {@link BroadcastStateTransformation}.
 */
@Internal
public class BroadcastStateTransformationTranslator<IN1, IN2, OUT>
		extends AbstractTwoInputTransformationTranslator<IN1, IN2, OUT, BroadcastStateTransformation<IN1, IN2, OUT>> {

	@Override
	protected Collection<Integer> translateForBatchInternal(
			final BroadcastStateTransformation<IN1, IN2, OUT> transformation,
			final Context context) {
		throw new UnsupportedOperationException("The Broadcast State Pattern is not support in BATCH execution mode.");
	}

	@Override
	protected Collection<Integer> translateForStreamingInternal(
			final BroadcastStateTransformation<IN1, IN2, OUT> transformation,
			final Context context) {
		checkNotNull(transformation);
		checkNotNull(context);

		return translateInternal(
				transformation,
				transformation.getNonBroadcastStream(),
				transformation.getBroadcastStream(),
				transformation.getOperatorFactory(),
				transformation.getStateKeyType(),
				transformation.getKeySelector(),
				null,
				context);
	}
}
