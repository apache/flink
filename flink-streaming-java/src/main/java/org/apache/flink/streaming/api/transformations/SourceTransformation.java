/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link PhysicalTransformation} for {@link Source}.
 */
@Internal
public class SourceTransformation<OUT> extends PhysicalTransformation<OUT> implements WithBoundedness {
	private final SourceOperatorFactory<OUT> sourceFactory;
	/**
	 * Creates a new {@code Transformation} with the given name, output type and parallelism.
	 *
	 * @param name            The name of the {@code Transformation}, this will be shown in Visualizations and the Log
	 * @param sourceFactory   The operator factory for {@link SourceOperator}.
	 * @param outputType      The output type of this {@code Transformation}
	 * @param parallelism     The parallelism of this {@code Transformation}
	 */
	public SourceTransformation(
			String name,
			SourceOperatorFactory<OUT> sourceFactory,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.sourceFactory = sourceFactory;
	}

	@Override
	public Boundedness getBoundedness() {
		return sourceFactory.getBoundedness();
	}

	/**
	 * Returns the {@code StreamOperatorFactory} of this {@code LegacySourceTransformation}.
	 */
	public SourceOperatorFactory<OUT> getOperatorFactory() {
		return sourceFactory;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		return Collections.singleton(this);
	}

	@Override
	public List<Transformation<?>> getInputs() {
		return Collections.emptyList();
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		sourceFactory.setChainingStrategy(strategy);
	}
}
