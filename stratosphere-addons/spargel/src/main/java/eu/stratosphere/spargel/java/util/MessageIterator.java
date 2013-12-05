/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.spargel.java.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;


public final class MessageIterator<Message extends Value> implements Iterator<Message> {

	private final Message instance;
	private Iterator<PactRecord> source;
	
	public MessageIterator(Message instance) {
		this.instance = instance;
	}
	
	public final void setSource(Iterator<PactRecord> source) {
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
}