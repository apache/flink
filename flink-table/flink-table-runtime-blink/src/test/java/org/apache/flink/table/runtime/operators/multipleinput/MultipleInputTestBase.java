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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.output.BlackHoleOutput;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * A class that provides utility methods for multiple input testing.
 */
public class MultipleInputTestBase {

	protected Transformation<RowData> createSource(StreamExecutionEnvironment env, String... data) {
		return env.fromCollection(
				Arrays.stream(data).map(StringData::fromString).map(GenericRowData::of).collect(Collectors.toList()),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())))
				.getTransformation();
	}

	protected TestingOneInputStreamOperator createOneInputStreamOperator() throws Exception {
		TestingOneInputStreamOperator op = new TestingOneInputStreamOperator();
		op.setup(createStreamTask(), createStreamConfig(), new BlackHoleOutput());
		return op;
	}

	protected TestingTwoInputStreamOperator createTwoInputStreamOperator() throws Exception {
		TestingTwoInputStreamOperator op = new TestingTwoInputStreamOperator();
		op.setup(createStreamTask(), createStreamConfig(), new BlackHoleOutput());
		return op;
	}

	protected OneInputTransformation<RowData, RowData> createOneInputTransform(
			Transformation<RowData> input,
			String name,
			TypeInformation<RowData> outputType) {
		return createOneInputTransform(
				input,
				name,
				new TestingOneInputStreamOperator(),
				outputType);
	}

	protected OneInputTransformation<RowData, RowData> createOneInputTransform(
			Transformation<RowData> input,
			String name,
			TestingOneInputStreamOperator operator,
			TypeInformation<RowData> outputType) {
		return new OneInputTransformation<>(
				input,
				name,
				operator,
				outputType,
				10);
	}

	protected TwoInputTransformation<RowData, RowData, RowData> createTwoInputTransform(
			Transformation<RowData> input1,
			Transformation<RowData> input2,
			String name,
			TypeInformation<RowData> outputType) {
		return createTwoInputTransform(
				input1,
				input2,
				name,
				new TestingTwoInputStreamOperator(),
				outputType);
	}

	protected TwoInputTransformation<RowData, RowData, RowData> createTwoInputTransform(
			Transformation<RowData> input1,
			Transformation<RowData> input2,
			String name,
			TestingTwoInputStreamOperator operator,
			TypeInformation<RowData> outputType) {
		return new TwoInputTransformation<>(
				input1,
				input2,
				name,
				operator,
				outputType,
				10);
	}

	protected TableOperatorWrapper<TestingOneInputStreamOperator> createOneInputOperatorWrapper(
			TestingOneInputStreamOperator operator, String name) {
		return new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(operator),
				name,
				Collections.singletonList(new RowTypeInfo(Types.STRING)),
				new RowTypeInfo(Types.STRING)
		);
	}

	protected TableOperatorWrapper<TestingOneInputStreamOperator> createOneInputOperatorWrapper(String name) {
		return createOneInputOperatorWrapper(new TestingOneInputStreamOperator(), name);
	}

	protected TableOperatorWrapper<TestingTwoInputStreamOperator> createTwoInputOperatorWrapper(
			TestingTwoInputStreamOperator operator, String name) {
		return new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(operator),
				name,
				Arrays.asList(new RowTypeInfo(Types.STRING), new RowTypeInfo(Types.STRING)),
				new RowTypeInfo(Types.STRING, Types.STRING)
		);
	}

	protected TableOperatorWrapper<TestingTwoInputStreamOperator> createTwoInputOperatorWrapper(String name) {
		return createTwoInputOperatorWrapper(new TestingTwoInputStreamOperator(), name);
	}

	protected StreamOperatorParameters<RowData> createStreamOperatorParameters() throws Exception {
		return createStreamOperatorParameters(new CollectorOutput<>(new ArrayList<>()));
	}

	protected StreamConfig createStreamConfig() {
		return new MockStreamConfig(new Configuration(), 1);
	}

	protected StreamTask createStreamTask() throws Exception {
		Environment env = new MockEnvironmentBuilder().build();
		return new MockStreamTaskBuilder(env).build();
	}

	protected StreamOperatorParameters<RowData> createStreamOperatorParameters(
			CollectorOutput<RowData> output) throws Exception {
		return new StreamOperatorParameters<>(
				createStreamTask(),
				createStreamConfig(),
				output,
				TestProcessingTimeService::new,
				null
		);
	}
}
