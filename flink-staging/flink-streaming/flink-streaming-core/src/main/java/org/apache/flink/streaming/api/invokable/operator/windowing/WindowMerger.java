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

package org.apache.flink.streaming.api.invokable.operator.windowing;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.invokable.ChainableInvokable;

public class WindowMerger<T> extends ChainableInvokable<StreamWindow<T>, StreamWindow<T>> {

	private Map<Integer, StreamWindow<T>> windows;

	public WindowMerger() {
		super(null);
		this.windows = new HashMap<Integer, StreamWindow<T>>();
	}

	private static final long serialVersionUID = 1L;

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void callUserFunction() throws Exception {
		StreamWindow<T> nextWindow = nextObject;

		StreamWindow<T> current = windows.get(nextWindow.windowID);

		if (current == null) {
			current = nextWindow;
		} else {
			current = StreamWindow.merge(current, nextWindow);
		}

		if (current.numberOfParts == 1) {
			collector.collect(current);
			windows.remove(nextWindow.windowID);
		} else {
			windows.put(nextWindow.windowID, current);
		}
	}

	@Override
	public void collect(StreamWindow<T> record) {
		nextObject = record;
		callUserFunctionAndLogException();
	}
}
