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

package org.apache.flink.streaming.api.windowing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.util.Collector;

/**
 * Core abstraction for representing windows for {@link WindowedDataStream}s.
 * The user can apply transformations on these windows with the appropriate
 * {@link WindowedDataStream} methods. </p> Each stream window consists of a
 * random ID, a number representing the number of partitions for this specific
 * window (ID) and the elements itself. The ID and number of parts will be used
 * to merge the subwindows after distributed transformations.
 */
public class StreamWindow<T> extends ArrayList<T> implements Collector<T> {

	private static final long serialVersionUID = -5150196421193988403L;
	private static Random rnd = new Random();

	public int windowID;
	public int numberOfParts;

	/**
	 * Creates a new window with a random id
	 */
	public StreamWindow() {
		this(rnd.nextInt(), 1);
	}

	/**
	 * Creates a new window with the specific id
	 * 
	 * @param windowID
	 *            ID of the window
	 */
	public StreamWindow(int windowID) {
		this(windowID, 1);
	}

	/**
	 * Creates a new window with the given id and number of parts
	 * 
	 * @param windowID
	 * @param numberOfParts
	 */
	public StreamWindow(int windowID, int numberOfParts) {
		super();
		this.windowID = windowID;
		this.numberOfParts = numberOfParts;
	}

	/**
	 * Creates a shallow copy of the window
	 * 
	 * @param window
	 *            The window to be copied
	 */
	public StreamWindow(StreamWindow<T> window) {
		this(window.windowID, window.numberOfParts);
		addAll(window);
	}

	/**
	 * Creates a deep copy of the window using the given serializer
	 * 
	 * @param window
	 *            The window to be copied
	 * @param serializer
	 *            The serializer used for copying the records.
	 */
	public StreamWindow(StreamWindow<T> window, TypeSerializer<T> serializer) {
		this(window.windowID, window.numberOfParts);
		for (T element : window) {
			add(serializer.copy(element));
		}
	}

	/**
	 * Partitions the window using the given keyselector. A subwindow will be
	 * created for each key.
	 * 
	 * @param streamWindow
	 *            StreamWindow instance to partition
	 * @param keySelector
	 *            The keyselector used for extracting keys.
	 * @param withKey
	 *            Flag to decide whether the key object should be included in
	 *            the created window
	 * @return A list of the subwindows
	 */
	public static <X> List<StreamWindow<X>> partitionBy(StreamWindow<X> streamWindow,
			KeySelector<X, ?> keySelector, boolean withKey) throws Exception {
		Map<Object, StreamWindow<X>> partitions = new HashMap<Object, StreamWindow<X>>();

		for (X value : streamWindow) {
			Object key = keySelector.getKey(value);
			StreamWindow<X> window = partitions.get(key);
			if (window == null) {
				window = new StreamWindow<X>(streamWindow.windowID, 0);
				partitions.put(key, window);
			}
			window.add(value);
		}

		List<StreamWindow<X>> output = new ArrayList<StreamWindow<X>>();
		int numkeys = partitions.size();

		for (StreamWindow<X> window : partitions.values()) {
			output.add(window.setNumberOfParts(numkeys));
		}

		return output;
	}

	/**
	 * Splits the window into n equal (if possible) sizes.
	 * 
	 * @param window
	 *            Window to split
	 * @param n
	 *            Number of desired partitions
	 * @return The list of subwindows.
	 */
	public static <X> List<StreamWindow<X>> split(StreamWindow<X> window, int n) {
		int numElements = window.size();
		if (n == 0) {
			return new ArrayList<StreamWindow<X>>();
		}
		if (n > numElements) {
			return split(window, numElements);
		} else {
			List<StreamWindow<X>> splitsList = new ArrayList<StreamWindow<X>>();
			int splitSize = numElements / n;

			int index = -1;

			StreamWindow<X> currentSubWindow = new StreamWindow<X>(window.windowID, n);
			splitsList.add(currentSubWindow);

			for (X element : window) {
				index++;
				if (index == splitSize && splitsList.size() < n) {
					currentSubWindow = new StreamWindow<X>(window.windowID, n);
					splitsList.add(currentSubWindow);
					index = 0;
				}
				currentSubWindow.add(element);
			}
			return splitsList;
		}
	}

	public StreamWindow<T> setNumberOfParts(int n) {
		this.numberOfParts = n;
		return this;
	}

	public void setID(int id) {
		this.windowID = id;
	}

	/**
	 * Checks whether this window can be merged with the given one.
	 * 
	 * @param otherWindow
	 *            The window to test
	 * @return Window compatibility
	 */
	public boolean compatibleWith(StreamWindow<T> otherWindow) {
		return this.windowID == otherWindow.windowID && this.numberOfParts > 1;
	}

	/**
	 * Merges compatible windows together.
	 * 
	 * @param windows
	 *            Windows to merge
	 * @return Merged window
	 */
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

	/**
	 * Merges compatible windows together.
	 * 
	 * @param windows
	 *            Windows to merge
	 * @return Merged window
	 */
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
	public boolean equals(Object o) {
		return super.equals(o);
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
		return super.toString();
	}

	/**
	 * Creates a new {@link StreamWindow} with random id from the given elements
	 * 
	 * @param elements
	 *            The elements contained in the resulting window
	 * @return The window
	 */
	public static <R> StreamWindow<R> fromElements(R... elements) {
		StreamWindow<R> window = new StreamWindow<R>();
		for (R element : elements) {
			window.add(element);
		}
		return window;
	}
}
