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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Basic window buffer that stores the elements in a simple list without any
 * pre-aggregation.
 */
public class BasicWindowBuffer<T> extends WindowBuffer<T> {

	private static final long serialVersionUID = 1L;
	protected LinkedList<T> buffer;

	public BasicWindowBuffer() {
		this.buffer = new LinkedList<T>();
	}

	public void emitWindow(Collector<StreamRecord<StreamWindow<T>>> collector) {
		if (emitEmpty || !buffer.isEmpty()) {
			StreamWindow<T> currentWindow = createEmptyWindow();
			currentWindow.addAll(buffer);
			collector.collect(new StreamRecord<StreamWindow<T>>(currentWindow));
		} 
	}

	public void store(T element) throws Exception {
		buffer.add(element);
	}

	public void evict(int n) {
		for (int i = 0; i < n; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}
	}

	@Override
	public BasicWindowBuffer<T> clone() {
		return new BasicWindowBuffer<T>();
	}

	@Override
	public String toString() {
		return buffer.toString();
	}
}
