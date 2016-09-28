/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

@Internal
public class KeyedStreamFilter<K, IN> extends AbstractKeyedOneInputStreamOperator<K, IN, IN> {

	private static final long serialVersionUID = 1L;

	private final FilterFunction<IN> filterFunction;

	public KeyedStreamFilter(
			FilterFunction<IN> filterFunction,
			TypeSerializer<K> keySerializer,
			KeySelector<IN, K> keySelector) {
		super(filterFunction, keySerializer, keySelector);
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.filterFunction = filterFunction;
	}

	@Override
	public void processKeyedElement(K key, StreamRecord<IN> element) throws Exception {
		if (filterFunction.filter(element.getValue())) {
			output.collect(element);
		}
	}
}
