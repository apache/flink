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

package org.apache.flink.streaming.runtime.operators.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;


public class AggregatingKeyedTimePanes<Type, Key> extends AbstractKeyedTimePanes<Type, Key, Type, Type> {
	
	private final KeySelector<Type, Key> keySelector;
	
	private final ReduceFunction<Type> reducer;
	
	private long evaluationPass;

	// ------------------------------------------------------------------------
	
	public AggregatingKeyedTimePanes(KeySelector<Type, Key> keySelector, ReduceFunction<Type> reducer) {
		this.keySelector = keySelector;
		this.reducer = reducer;
	}

	// ------------------------------------------------------------------------

	@Override
	public void addElementToLatestPane(Type element) throws Exception {
		Key k = keySelector.getKey(element);
		latestPane.putOrAggregate(k, element, reducer);
	}

	@Override
	public void evaluateWindow(Collector<Type> out) throws Exception {
		if (previousPanes.isEmpty()) {
			// optimized path for single pane case
			for (KeyMap.Entry<Key, Type> entry : latestPane) {
				out.collect(entry.getValue());
			}
		}
		else {
			// general code path for multi-pane case
			AggregatingTraversal<Key, Type> evaluator = new AggregatingTraversal<>(reducer, out);
			traverseAllPanes(evaluator, evaluationPass);
		}
		
		evaluationPass++;
	}

	// ------------------------------------------------------------------------
	//  The maps traversal that performs the final aggregation
	// ------------------------------------------------------------------------
	
	static final class AggregatingTraversal<Key, Type> implements KeyMap.TraversalEvaluator<Key, Type> {

		private final ReduceFunction<Type> function;
		
		private final Collector<Type> out;
		
		private Type currentValue;

		AggregatingTraversal(ReduceFunction<Type> function, Collector<Type> out) {
			this.function = function;
			this.out = out;
		}

		@Override
		public void startNewKey(Key key) {
			currentValue = null;
		}

		@Override
		public void nextValue(Type value) throws Exception {
			if (currentValue != null) {
				currentValue = function.reduce(currentValue, value);
			}
			else {
				currentValue = value;
			}
		}

		@Override
		public void keyDone() throws Exception {
			out.collect(currentValue);
		}
	}
}
