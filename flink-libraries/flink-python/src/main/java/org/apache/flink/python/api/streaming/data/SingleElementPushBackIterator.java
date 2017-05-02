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

package org.apache.flink.python.api.streaming.data;

import org.apache.flink.util.Preconditions;

import java.util.Iterator;

/**
 * This class is a wrapper for an {@link Iterator} that supports pushing back a single record.
 *
 * @param <IN> input type
 */
class SingleElementPushBackIterator<IN> {

	private IN pushBack;
	private final Iterator<IN> iterator;

	SingleElementPushBackIterator(Iterator<IN> iterator) {
		this.pushBack = null;
		this.iterator = iterator;
	}

	public boolean hasNext() {
		return pushBack != null || iterator.hasNext();
	}

	public IN next() {
		if (pushBack != null) {
			IN obj = pushBack;
			pushBack = null;
			return obj;
		} else {
			return iterator.next();
		}
	}

	public void pushBack(IN element) {
		Preconditions.checkState(pushBack == null, "Already contains an element that was pushed back. This indicates a programming error.");
		pushBack = element;
	}
}
