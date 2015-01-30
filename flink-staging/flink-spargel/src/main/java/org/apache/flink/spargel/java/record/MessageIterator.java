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

package org.apache.flink.spargel.java.record;

import java.util.Iterator;

import org.apache.flink.types.Record;
import org.apache.flink.types.Value;

public final class MessageIterator<Message extends Value> implements Iterator<Message>, Iterable<Message> {

	private final Message instance;
	private Iterator<Record> source;
	
	public MessageIterator(Message instance) {
		this.instance = instance;
	}
	
	public final void setSource(Iterator<Record> source) {
		this.source = source;
	}
	
	@Override
	public final boolean hasNext() {
		return this.source.hasNext();
	}
	
	@Override
	public final Message next() {
		this.source.next().getFieldInto(1, this.instance);
		return this.instance;
	}

	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Message> iterator() {
		return this;
	}
}
