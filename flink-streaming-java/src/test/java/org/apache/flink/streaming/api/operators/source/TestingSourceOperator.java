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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

/**
 * A SourceOperator extension to simplify test setup.
 */
public class TestingSourceOperator<T>  extends SourceOperator<T, MockSourceSplit> {

	private static final long serialVersionUID = 1L;

	private final int subtaskIndex;
	private final int parallelism;

	public TestingSourceOperator(
			SourceReader<T, MockSourceSplit> reader,
			WatermarkStrategy<T> watermarkStrategy,
			ProcessingTimeService timeService) {

		this(reader, watermarkStrategy, timeService, new MockOperatorEventGateway(), 1, 5);
	}

	public TestingSourceOperator(
			SourceReader<T, MockSourceSplit> reader,
			OperatorEventGateway eventGateway,
			int subtaskIndex) {

		this(reader, WatermarkStrategy.noWatermarks(), new TestProcessingTimeService(), eventGateway, subtaskIndex, 5);
	}

	public TestingSourceOperator(
			SourceReader<T, MockSourceSplit> reader,
			WatermarkStrategy<T> watermarkStrategy,
			ProcessingTimeService timeService,
			OperatorEventGateway eventGateway,
			int subtaskIndex,
			int parallelism) {

		super(
			(context) -> reader,
			eventGateway,
			new MockSourceSplitSerializer(),
			watermarkStrategy,
			timeService);

		this.subtaskIndex = subtaskIndex;
		this.parallelism = parallelism;
		this.metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
	}

	@Override
	public StreamingRuntimeContext getRuntimeContext() {
		return new MockStreamingRuntimeContext(false, parallelism, subtaskIndex);
	}

	// this is overridden to avoid complex mock injection through the "containingTask"
	@Override
	public ExecutionConfig getExecutionConfig() {
		ExecutionConfig cfg = new ExecutionConfig();
		cfg.setAutoWatermarkInterval(100);
		return cfg;
	}
}
