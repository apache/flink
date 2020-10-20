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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractMultipleInputTransformation} implementation for keyed streams.
 */
@Internal
public class KeyedMultipleInputTransformation<OUT> extends AbstractMultipleInputTransformation<OUT> {
	private final List<KeySelector<?, ?>> stateKeySelectors = new ArrayList<>();
	protected final TypeInformation<?> stateKeyType;

	public KeyedMultipleInputTransformation(
			String name,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<OUT> outputType,
			int parallelism,
			TypeInformation<?> stateKeyType) {
		super(name, operatorFactory, outputType, parallelism);
		this.stateKeyType = stateKeyType;
		updateManagedMemoryStateBackendUseCase(true);
	}

	public KeyedMultipleInputTransformation<OUT> addInput(Transformation<?> input, KeySelector<?, ?> keySelector) {
		inputs.add(input);
		getStateKeySelectors().add(keySelector);
		return this;
	}

	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	public List<KeySelector<?, ?>> getStateKeySelectors() {
		return stateKeySelectors;
	}
}
