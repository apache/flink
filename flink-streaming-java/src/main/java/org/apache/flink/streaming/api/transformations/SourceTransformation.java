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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.Collection;
import java.util.Collections;

/**
 * This represents a Source. This does not actually transform anything since it has no inputs but
 * it is the root {@code Transformation} of any topology.
 *
 * @param <T> The type of the elements that this source produces
 */
@Internal
public class SourceTransformation<T> extends PhysicalTransformation<T> {

	private final StreamOperatorFactory<T> operatorFactory;

	/**
	 * Creates a new {@code SourceTransformation} from the given operator.
	 *
	 * @param name The name of the {@code SourceTransformation}, this will be shown in Visualizations and the Log
	 * @param operator The {@code StreamSource} that is the operator of this Transformation
	 * @param outputType The type of the elements produced by this {@code SourceTransformation}
	 * @param parallelism The parallelism of this {@code SourceTransformation}
	 */
	public SourceTransformation(
			String name,
			StreamSource<T, ?> operator,
			TypeInformation<T> outputType,
			int parallelism) {
		this(name, SimpleOperatorFactory.of(operator), outputType, parallelism);
	}

	public SourceTransformation(
			String name,
			StreamOperatorFactory<T> operatorFactory,
			TypeInformation<T> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.operatorFactory = operatorFactory;
	}

	@VisibleForTesting
	public StreamSource<T, ?> getOperator() {
		return (StreamSource<T, ?>) ((SimpleOperatorFactory) operatorFactory).getOperator();
	}

	/**
	 * Returns the {@code StreamOperatorFactory} of this {@code SourceTransformation}.
	 */
	public StreamOperatorFactory<T> getOperatorFactory() {
		return operatorFactory;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		return Collections.singleton(this);
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operatorFactory.setChainingStrategy(strategy);
	}
}
