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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class GroupedFoldInvokable<IN, OUT> extends StreamFoldInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private Map<Object, OUT> values;
	private OUT folded;
	private OUT initialValue;

	public GroupedFoldInvokable(FoldFunction<OUT, IN> folder, KeySelector<IN, ?> keySelector, OUT initialValue) {
		super(folder, initialValue);
		this.keySelector = keySelector;
		this.initialValue = initialValue;
		values = new HashMap<Object, OUT>();
	}

	@Override
	protected void fold() throws Exception {
		Object key = nextRecord.getKey(keySelector);
		accumulator = values.get(key);
		nextValue = nextObject;
		if (accumulator != null) {
			callUserFunctionAndLogException();
			values.put(key, folded);
			collector.collect(folded);
		} else {
			values.put(key, initialValue);
			collector.collect(initialValue);
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		folded = folder.fold(accumulator, nextValue);
	}

}
