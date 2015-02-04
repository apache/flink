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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

public class StreamWindow<T> extends ArrayList<T> implements Collector<T> {

	private static final long serialVersionUID = -5150196421193988403L;
	private static Random rnd = new Random();

	public int windowID;
	public int transformationID;

	public int numberOfParts;

	public StreamWindow() {
		this(rnd.nextInt(), rnd.nextInt(), 1);
	}

	public StreamWindow(int windowID) {
		this(windowID, rnd.nextInt(), 1);
	}

	public StreamWindow(int windowID, int transformationID, int numberOfParts) {
		super();
		this.windowID = windowID;
		this.transformationID = transformationID;
		this.numberOfParts = numberOfParts;
	}

	public StreamWindow(StreamWindow<T> window) {
		this(window.windowID, window.transformationID, window.numberOfParts);
		addAll(window);
	}

	public StreamWindow(StreamWindow<T> window, TypeSerializer<T> serializer) {
		this(window.windowID, window.transformationID, window.numberOfParts);
		for (T element : window) {
			add(serializer.copy(element));
		}
	}

	public List<StreamWindow<T>> partitionBy(KeySelector<T, ?> keySelector) throws Exception {
		Map<Object, StreamWindow<T>> partitions = new HashMap<Object, StreamWindow<T>>();

		for (T value : this) {
			Object key = keySelector.getKey(value);
			StreamWindow<T> window = partitions.get(key);
			if (window == null) {
				window = new StreamWindow<T>(this.windowID, this.transformationID, 0);
				partitions.put(key, window);
			}
			window.add(value);
		}

		List<StreamWindow<T>> output = new ArrayList<StreamWindow<T>>();
		int numkeys = partitions.size();

		for (StreamWindow<T> window : partitions.values()) {
			output.add(window.setNumberOfParts(numkeys));
		}

		return output;
	}

	public List<StreamWindow<T>> split(int n) {
		List<List<T>> subLists = Lists.partition(this, (int) Math.ceil((double) size() / n));
		List<StreamWindow<T>> split = new ArrayList<StreamWindow<T>>(n);
		for (List<T> partition : subLists) {
			StreamWindow<T> subWindow = new StreamWindow<T>(windowID, transformationID,
					subLists.size());
			subWindow.addAll(partition);
			split.add(subWindow);
		}
		return split;
	}

	public StreamWindow<T> setNumberOfParts(int n) {
		this.numberOfParts = n;
		return this;
	}

	public boolean compatibleWith(StreamWindow<T> otherWindow) {
		return this.windowID == otherWindow.windowID && this.numberOfParts > 1;
	}

	public static <R> StreamWindow<R> merge(StreamWindow<R>... windows) {
		StreamWindow<R> window = new StreamWindow<R>(windows[0]);
		for (int i = 1; i < windows.length; i++) {
			StreamWindow<R> next = windows[i];
			if (window.compatibleWith(next)) {
				window.addAll(next);
				window.numberOfParts--;
			} else {
				throw new RuntimeException("Can only merge compatible windows");
			}
		}
		return window;
	}

	public static <R> StreamWindow<R> merge(List<StreamWindow<R>> windows) {
		if (windows.isEmpty()) {
			throw new RuntimeException("Need at least one window to merge");
		} else {
			StreamWindow<R> window = new StreamWindow<R>(windows.get(0));
			for (int i = 1; i < windows.size(); i++) {
				StreamWindow<R> next = windows.get(i);
				if (window.compatibleWith(next)) {
					window.addAll(next);
					window.numberOfParts--;
				} else {
					throw new RuntimeException("Can only merge compatible windows");
				}
			}
			return window;
		}
	}

	@Override
	public void collect(T record) {
		add(record);
	}

	@Override
	public void close() {
	}

	@Override
	public String toString() {
		return super.toString() + " " + windowID + " (" + numberOfParts + ")";
	}
}
