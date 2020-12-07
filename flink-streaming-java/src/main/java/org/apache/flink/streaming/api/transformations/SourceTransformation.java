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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PhysicalTransformation} for {@link Source}.
 */
@Internal
public class SourceTransformation<OUT, SplitT extends SourceSplit, EnumChkT> extends PhysicalTransformation<OUT> implements WithBoundedness {

	private final Source<OUT, SplitT, EnumChkT> source;
	private final WatermarkStrategy<OUT> watermarkStrategy;

	private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

	/**
	 * Creates a new {@code Transformation} with the given name, output type and parallelism.
	 *
	 * @param name The name of the {@code Transformation}, this will be shown in Visualizations
	 * 		and the Log
	 * @param source The {@link Source} itself
	 * @param watermarkStrategy The {@link WatermarkStrategy} to use
	 * @param outputType The output type of this {@code Transformation}
	 * @param parallelism The parallelism of this {@code Transformation}
	 */
	public SourceTransformation(
			String name,
			Source<OUT, SplitT, EnumChkT> source,
			WatermarkStrategy<OUT> watermarkStrategy,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.source = source;
		this.watermarkStrategy = watermarkStrategy;
	}

	public Source<OUT, SplitT, EnumChkT> getSource() {
		return source;
	}

	public WatermarkStrategy<OUT> getWatermarkStrategy() {
		return watermarkStrategy;
	}

	@Override
	public Boundedness getBoundedness() {
		return source.getBoundedness();
	}

	@Override
	public List<Transformation<?>> getTransitivePredecessors() {
		return Collections.singletonList(this);
	}

	@Override
	public List<Transformation<?>> getInputs() {
		return Collections.emptyList();
	}

	@Override
	public void setChainingStrategy(ChainingStrategy chainingStrategy) {
		this.chainingStrategy = checkNotNull(chainingStrategy);
	}

	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}
}
