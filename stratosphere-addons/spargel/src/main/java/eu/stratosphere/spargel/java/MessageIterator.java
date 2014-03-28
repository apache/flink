/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.spargel.java;

import java.util.Iterator;

import eu.stratosphere.api.java.tuple.Tuple2;

public final class MessageIterator<Message> implements Iterator<Message>, Iterable<Message>, java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private transient Iterator<Tuple2<?, Message>> source;
	
	
	final void setSource(Iterator<Tuple2<?, Message>> source) {
		this.source = source;
	}
	
	@Override
	public final boolean hasNext() {
		return this.source.hasNext();
	}
	
	@Override
	public final Message next() {
		return this.source.next().f1;
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