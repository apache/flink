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

import org.apache.flink.streaming.api.operators.ChainableStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * This operator manages the window buffers attached to the discretizers.
 */
public class StreamWindowBuffer<T> extends ChainableStreamOperator<WindowEvent<T>, StreamWindow<T>> {

	protected WindowBuffer<T> buffer;

	public StreamWindowBuffer(WindowBuffer<T> buffer) {
		super(null);
		this.buffer = buffer;
		withoutInputCopy();
	}

	private static final long serialVersionUID = 1L;

	@Override
	public void run() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		handleWindowEvent(nextObject);
	}

	protected void handleWindowEvent(WindowEvent<T> windowEvent, WindowBuffer<T> buffer)
			throws Exception {
		if (windowEvent.isElement()) {
			buffer.store(windowEvent.getElement());
		} else if (windowEvent.isEviction()) {
			buffer.evict(windowEvent.getEviction());
		} else if (windowEvent.isTrigger()) {
			buffer.emitWindow(collector);
		}
	}

	private void handleWindowEvent(WindowEvent<T> windowEvent) throws Exception {
		handleWindowEvent(windowEvent, buffer);
	}

}
