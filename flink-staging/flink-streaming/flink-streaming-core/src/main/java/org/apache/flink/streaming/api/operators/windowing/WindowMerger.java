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

package org.apache.flink.streaming.api.operators.windowing;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator merges together the different partitions of the
 * {@link StreamWindow}s used to merge the results of parallel transformations
 * that belong in the same window.
 */
public class WindowMerger<T> extends AbstractStreamOperator<StreamWindow<T>>
		implements OneInputStreamOperator<StreamWindow<T>, StreamWindow<T>> {

	private static final long serialVersionUID = 1L;

	private Map<Integer, StreamWindow<T>> windows;

	public WindowMerger() {
		this.windows = new HashMap<Integer, StreamWindow<T>>();

		chainingStrategy = ChainingStrategy.FORCE_ALWAYS;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamWindow<T> nextWindow) throws Exception {

		StreamWindow<T> current = windows.get(nextWindow.windowID);

		if (current == null) {
			current = nextWindow;
		} else {
			current = StreamWindow.merge(current, nextWindow);
		}

		if (current.numberOfParts == 1) {
			output.collect(current);
			windows.remove(nextWindow.windowID);
		} else {
			windows.put(nextWindow.windowID, current);
		}
	}
}
