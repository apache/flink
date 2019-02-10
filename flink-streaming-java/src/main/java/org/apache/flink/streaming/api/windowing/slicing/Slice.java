/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.slicing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Output type of a process result from sliced stream.
 *
 * <p>A slice is defined by its contents shared key and window
 * This slice container also contains the combined contents
 *
 * @param <T> type of contents contains within the slice
 * @param <K> key type of the elements
 * @param <W> window type of the slice
 */
public class Slice<T, K, W extends Window> extends Tuple3<T, K, W> {

	public Slice() {
		super();
	}

	public Slice(T content, K key, W window) {
		super(content, key, window);
	}

	public T getContent() {
		return super.f0;
	}

	public void setContent(T content) {
		super.f0 = content;
	}

	public W getWindow() {
		return super.f2;
	}

	public void setWindow(W window) {
		super.f2 = window;
	}

	public K getKey() {
		return super.f1;
	}

	public void setKey(K key) {
		super.f1 = key;
	}
}
