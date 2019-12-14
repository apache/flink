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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

/**
 * The factory of {@link WatermarkAssignerOperator}.
 */
public class WatermarkAssignerOperatorFactory implements OneInputStreamOperatorFactory<BaseRow, BaseRow> {
	private static final long serialVersionUID = 1L;

	private final int rowtimeFieldIndex;

	private final long idleTimeout;

	private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

	private ChainingStrategy strategy = ChainingStrategy.ALWAYS;

	public WatermarkAssignerOperatorFactory(
			int rowtimeFieldIndex,
			long idleTimeout,
			GeneratedWatermarkGenerator generatedWatermarkGenerator) {
		this.rowtimeFieldIndex = rowtimeFieldIndex;
		this.idleTimeout = idleTimeout;
		this.generatedWatermarkGenerator = generatedWatermarkGenerator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamOperator createStreamOperator(StreamTask containingTask, StreamConfig config, Output output) {
		WatermarkGenerator watermarkGenerator = generatedWatermarkGenerator.newInstance(containingTask.getUserCodeClassLoader());
		WatermarkAssignerOperator operator = new WatermarkAssignerOperator(rowtimeFieldIndex, watermarkGenerator, idleTimeout);
		operator.setup(containingTask, config, output);
		return operator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		this.strategy = strategy;
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return strategy;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return WatermarkAssignerOperator.class;
	}
}
