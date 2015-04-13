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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * This operator flattens the results of the window transformations by
 * outputing the elements of the {@link StreamWindow} one-by-one
 */
public class GroupedWindowBuffer<T> extends StreamWindowBuffer<T> {

	private static final long serialVersionUID = 1L;
	private Map<Object, WindowBuffer<T>> windowMap = new HashMap<Object, WindowBuffer<T>>();
	private KeySelector<T, ?> keySelector;

	public GroupedWindowBuffer(WindowBuffer<T> buffer, KeySelector<T, ?> keySelector) {
		super(buffer);
		this.keySelector = keySelector;
	}

	@Override
	public void run() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		if (nextObject.getElement() != null) {
			Object key = keySelector.getKey(nextObject.getElement());
			WindowBuffer<T> currentWindow = windowMap.get(key);

			if (currentWindow == null) {
				currentWindow = buffer.clone();
				windowMap.put(key, currentWindow);
			}

			handleWindowEvent(nextObject, currentWindow);
		}
	}

	@Override
	public void collect(WindowEvent<T> record) {
		if (isRunning) {
			nextObject = record;
			callUserFunctionAndLogException();
		}
	}

}
