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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

public class StreamGroupedFold<IN, OUT> extends StreamFold<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private Map<Object, OUT> values;
	private OUT initialValue;

	public StreamGroupedFold(FoldFunction<IN, OUT> folder, KeySelector<IN, ?> keySelector,
			OUT initialValue, TypeInformation<OUT> outTypeInformation) {
		super(folder, initialValue, outTypeInformation);
		this.keySelector = keySelector;
		this.initialValue = initialValue;
		values = new HashMap<Object, OUT>();
	}

	@Override
	public void processElement(IN element) throws Exception {
		Object key = keySelector.getKey(element);
		OUT accumulator = values.get(key);
		FoldFunction<IN, OUT> folder = ((FoldFunction<IN, OUT>) userFunction);

		if (accumulator != null) {
			OUT folded = folder.fold(outTypeSerializer.copy(accumulator), element);
			values.put(key, folded);
			output.collect(folded);
		} else {
			OUT first = folder.fold(outTypeSerializer.copy(initialValue), element);
			values.put(key, first);
			output.collect(first);
		}
	}

}
