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

package org.apache.flink.streaming.state;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;

/**
 * Simple wrapper class to convert an Iterator<StreamRecord<T>> to an
 * Iterator<T> iterator by invoking the getObject() method on every element.
 */
public class StreamIterator<T> implements Iterator<T>, Serializable {
	private static final long serialVersionUID = 1L;

	private Iterator<StreamRecord<T>> iterator = null;

	public void load(Iterator<StreamRecord<T>> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next().getObject();
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	@Override
	public String toString() {
		return iterator.toString();
	}

}
