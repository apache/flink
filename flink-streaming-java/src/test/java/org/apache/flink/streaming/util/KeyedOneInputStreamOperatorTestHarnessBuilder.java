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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * Builder of {@link KeyedOneInputStreamOperatorTestHarness}.
 *
 * @param <K> The key type of the operator.
 * @param <IN> The input type of the operator.
 * @param <OUT> The output type of the operator.
 */
public class KeyedOneInputStreamOperatorTestHarnessBuilder<K, IN, OUT> extends AbstractBasicStreamOperatorTestHarnessBuilder<KeyedOneInputStreamOperatorTestHarnessBuilder<K, IN, OUT>, OUT> {
	protected KeySelector<IN, K> keySelector;

	protected TypeInformation<K> keyType;

	public KeyedOneInputStreamOperatorTestHarnessBuilder<K, IN, OUT> setKeySelector(KeySelector<IN, K> keySelector) {
		this.keySelector = Preconditions.checkNotNull(keySelector);
		return this;
	}

	public KeyedOneInputStreamOperatorTestHarnessBuilder<K, IN, OUT> setKeyType(TypeInformation<K> keyType) {
		this.keyType = keyType;
		return this;
	}

	@Override
	public KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> build() throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(
			(OneInputStreamOperator<IN, OUT>) super.operator,
			super.computeFactoryIfAbsent(),
			keySelector,
			keyType,
			super.computeEnvironmentIfAbsent(),
			super.isInternalEnvironment,
			super.operatorID);
	}
}
