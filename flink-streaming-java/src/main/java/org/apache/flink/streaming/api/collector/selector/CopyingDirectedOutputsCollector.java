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

package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Map;

/**
 * Special version of {@link DirectedOutputsCollector} that performs a shallow copy of the
 * {@link StreamRecord} to ensure that multi-chaining works correctly.
 *
 * @param <T> The type of the elements that can be emitted.
 */
public class CopyingDirectedOutputsCollector<T> extends DirectedOutputsCollector<T> {

	public CopyingDirectedOutputsCollector(
			Output<StreamRecord<T>>[] selectAllOutputs,
			Map<String, Output<StreamRecord<T>>[]> outputMap) {
		super(selectAllOutputs, outputMap);
	}

	@Override
	protected void collect(Output<StreamRecord<T>>[] outputs, StreamRecord<T> record) {
		for (Output<StreamRecord<T>> output : outputs) {
			StreamRecord<T> shallowCopy = record.copy(record.getValue());
			output.collect(shallowCopy);
		}
	}
}
