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

package org.apache.flink.streaming.api.windowing;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Type representing events sent to the window buffer. The first field should
 * contain the window element the second field encodes triggers and evictions if
 * the second field is greater than 0 it represents an eviction if it equals -1
 * it represents a trigger.
 */
public class WindowEvent<T> extends Tuple2<T, Integer> {
	private static final long serialVersionUID = 1L;

	public boolean isElement() {
		return f1 == 0;
	}

	public boolean isEviction() {
		return f1 > 0;
	}

	public boolean isTrigger() {
		return f1 == -1;
	}

	public Integer getEviction() {
		return f1;
	}

	public T getElement() {
		return f0;
	}

	public WindowEvent<T> setElement(T element) {
		f0 = element;
		f1 = 0;
		return this;
	}

	public WindowEvent<T> setTrigger() {
		f1 = -1;
		return this;
	}

	public WindowEvent<T> setEviction(Integer n) {
		if (n > 0) {
			f1 = n;
			return this;
		} else {
			throw new RuntimeException("Must evict at least 1");
		}
	}
}
