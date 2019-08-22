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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.generated.GeneratedClass;

/**
 * Stream operator factory for code gen operator.
 */
public class CodeGenOperatorFactory<OUT> implements StreamOperatorFactory<OUT> {

	private final GeneratedClass<? extends StreamOperator<OUT>> generatedClass;
	private ChainingStrategy strategy = ChainingStrategy.ALWAYS;

	public CodeGenOperatorFactory(GeneratedClass<? extends StreamOperator<OUT>> generatedClass) {
		this.generatedClass = generatedClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamTask<?, ?> containingTask,
			StreamConfig config, Output<StreamRecord<OUT>> output) {
		return (T) generatedClass.newInstance(containingTask.getUserCodeClassLoader(),
				generatedClass.getReferences(), containingTask, config, output);
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
	public boolean isOperatorSelectiveReading(ClassLoader classLoader) {
		return InputSelectable.class.isAssignableFrom(getStreamOperatorClass(classLoader));
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return generatedClass.getClass(classLoader);
	}

	public GeneratedClass<? extends StreamOperator<OUT>> getGeneratedClass() {
		return generatedClass;
	}
}
