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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.util.UnionIterator;
import org.apache.flink.streaming.api.functions.windows.KeyedWindowFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


public class AccumulatingKeyedTimePanes<Type, Key, Result> extends AbstractKeyedTimePanes<Type, Key, ArrayList<Type>, Result> {
	
	private final KeySelector<Type, Key> keySelector;

	private final KeyMap.LazyFactory<ArrayList<Type>> listFactory = getListFactory();

	private final KeyedWindowFunction<Type, Result, Key> function;
	
	private long evaluationPass;

	// ------------------------------------------------------------------------
	
	public AccumulatingKeyedTimePanes(KeySelector<Type, Key> keySelector, KeyedWindowFunction<Type, Result, Key> function) {
		this.keySelector = keySelector;
		this.function = function;
	}

	// ------------------------------------------------------------------------

	@Override
	public void addElementToLatestPane(Type element) throws Exception {
		Key k = keySelector.getKey(element);
		ArrayList<Type> elements = latestPane.putIfAbsent(k, listFactory);
		elements.add(element);
	}

	@Override
	public void evaluateWindow(Collector<Result> out) throws Exception {
		if (previousPanes.isEmpty()) {
			// optimized path for single pane case (tumbling window)
			for (KeyMap.Entry<Key, ArrayList<Type>> entry : latestPane) {
				function.evaluate(entry.getKey(), entry.getValue(), out);
			}
		}
		else {
			// general code path for multi-pane case
			WindowFunctionTraversal<Key, Type, Result> evaluator = new WindowFunctionTraversal<>(function, out);
			traverseAllPanes(evaluator, evaluationPass);
		}
		
		evaluationPass++;
	}

	// ------------------------------------------------------------------------
	//  Running a window function in a map traversal
	// ------------------------------------------------------------------------
	
	static final class WindowFunctionTraversal<Key, Type, Result> implements KeyMap.TraversalEvaluator<Key, ArrayList<Type>> {

		private final KeyedWindowFunction<Type, Result, Key> function;
		
		private final UnionIterator<Type> unionIterator;
		
		private final Collector<Result> out;
		
		private Key currentKey;

		WindowFunctionTraversal(KeyedWindowFunction<Type, Result, Key> function, Collector<Result> out) {
			this.function = function;
			this.out = out;
			this.unionIterator = new UnionIterator<>();
		}


		@Override
		public void startNewKey(Key key) {
			unionIterator.clear();
			currentKey = key;
		}

		@Override
		public void nextValue(ArrayList<Type> value) {
			unionIterator.addList(value);
		}

		@Override
		public void keyDone() throws Exception {
			function.evaluate(currentKey, unionIterator, out);
		}
	}
	
	// ------------------------------------------------------------------------
	//  Lazy factory for lists (put if absent)
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	private static <V> KeyMap.LazyFactory<ArrayList<V>> getListFactory() {
		return (KeyMap.LazyFactory<ArrayList<V>>) LIST_FACTORY;
	}

	private static final KeyMap.LazyFactory<?> LIST_FACTORY = new KeyMap.LazyFactory<ArrayList<?>>() {

		@Override
		public ArrayList<?> create() {
			return new ArrayList<>(4);
		}
	};
}
