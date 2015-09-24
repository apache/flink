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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayDeque;


public abstract class AbstractKeyedTimePanes<Type, Key, Aggregate, Result> {
	
	protected KeyMap<Key, Aggregate> latestPane = new KeyMap<>();

	protected final ArrayDeque<KeyMap<Key, Aggregate>> previousPanes = new ArrayDeque<>();

	// ------------------------------------------------------------------------

	public abstract void addElementToLatestPane(Type element) throws Exception;

	public abstract void evaluateWindow(Collector<Result> out, TimeWindow window) throws Exception;
	
	
	public void dispose() {
		// since all is heap data, there is no need to clean up anything
		latestPane = null;
		previousPanes.clear();
	}
	
	
	public void slidePanes(int panesToKeep) {
		if (panesToKeep > 1) {
			// the current pane becomes the latest previous pane
			previousPanes.addLast(latestPane);

			// truncate the history
			while (previousPanes.size() >= panesToKeep) {
				previousPanes.removeFirst();
			}
		}

		// we need a new latest pane
		latestPane = new KeyMap<>();
	}
	
	public void truncatePanes(int numToRetain) {
		while (previousPanes.size() >= numToRetain) {
			previousPanes.removeFirst();
		}
	}
	
	protected void traverseAllPanes(KeyMap.TraversalEvaluator<Key, Aggregate> traversal, long traversalPass) throws Exception{
		// gather all panes in an array (faster iterations)
		@SuppressWarnings({"unchecked", "rawtypes"})
		KeyMap<Key, Aggregate>[] panes = previousPanes.toArray(new KeyMap[previousPanes.size() + 1]);
		panes[panes.length - 1] = latestPane;

		// let the maps make a coordinated traversal and evaluate the window function per contained key
		KeyMap.traverseMaps(panes, traversal, traversalPass);
	}
}
