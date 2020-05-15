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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link Transformation} that creates a physical operation. It enables setting {@link ChainingStrategy}.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 * @see Transformation
 */
@Internal
public abstract class PhysicalTransformation<T> extends Transformation<T> {

	/**
	 * Creates a new {@code Transformation} with the given name, output type and parallelism.
	 *
	 * @param name The name of the {@code Transformation}, this will be shown in Visualizations and the Log
	 * @param outputType The output type of this {@code Transformation}
	 * @param parallelism The parallelism of this {@code Transformation}
	 */
	PhysicalTransformation(
		String name,
		TypeInformation<T> outputType,
		int parallelism) {
		super(name, outputType, parallelism);
	}

	/**
	 * Sets the chaining strategy of this {@code Transformation}.
	 */
	public abstract void setChainingStrategy(ChainingStrategy strategy);

	/**
	 * Get an optional {@link org.apache.flink.runtime.operators.coordination.OperatorCoordinator.Provider Provider}
	 * of the {@link OperatorCoordinator}.
	 *
	 * <p>The input parameters of the coordinator provider factory is following:
	 * First Parameter: name of the operator, type: String.
	 * Second parameter: the operator id, type: {@link OperatorID}.
	 *
	 * @return The {@link org.apache.flink.runtime.operators.coordination.OperatorCoordinator.Provider Provider} of
	 * the {@link OperatorCoordinator}.
	 */
	public Optional<Function<Tuple2<String, OperatorID>, OperatorCoordinator.Provider>> getOperatorCoordinatorProviderFactory() {
		return Optional.empty();
	}
}
