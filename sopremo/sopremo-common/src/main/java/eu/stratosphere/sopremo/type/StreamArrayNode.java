/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Arvid Heise
 */
public class StreamArrayNode extends AbstractJsonNode implements IStreamArrayNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7639586637351342346L;

	private static final Collection<IJsonNode> EMPTY_SET = Collections.emptySet();

	/**
	 * 
	 */
	private static final Iterator<IJsonNode> EMPTY_ITERATOR = EMPTY_SET.iterator();

	private Iterator<IJsonNode> nodeIterator;

	public StreamArrayNode(Iterator<IJsonNode> nodeIterator) {
		this.nodeIterator = nodeIterator;
	}

	/**
	 * Initializes OneTimeArrayNode.
	 */
	public StreamArrayNode() {
		this(EMPTY_ITERATOR);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#getType()
	 */
	@Override
	public Type getType() {
		return Type.ArrayNode;
	}

	public Iterator<IJsonNode> getNodeIterator() {
		return this.nodeIterator;
	}

	public void setNodeIterator(Iterator<IJsonNode> nodeIterator) {
		if (nodeIterator == null)
			throw new NullPointerException("nodeIterator must not be null");

		this.nodeIterator = nodeIterator;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IStreamArrayNode#getFirst()
	 */
	@Override
	public IJsonNode getFirst() {
		return this.nodeIterator.next();
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#clear()
	 */
	@Override
	public void clear() {
		this.nodeIterator = EMPTY_ITERATOR;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return !this.nodeIterator.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<IJsonNode> iterator() {
		return this.nodeIterator;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#getJavaValue()
	 */
	@Override
	public Iterator<IJsonNode> getJavaValue() {
		return this.nodeIterator;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder sb) {
		sb.append('[');

		final Iterator<IJsonNode> iterator = this.iterator();
		if (iterator.hasNext()) {
			sb.append(iterator.next());

			for (int index = 0; iterator.hasNext() && index < 100; index++)
				sb.append(", ").append(iterator.next());
			
			if(iterator.hasNext())
				sb.append(", ...");
		}

		sb.append(']');
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#copyValueFrom(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public void copyValueFrom(IJsonNode otherNode) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("Use CoreFunctions#ALL to transform this stream array into a materialized array");
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#compareToSameType(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public int compareToSameType(IJsonNode other) {
		return System.identityHashCode(this) - System.identityHashCode(other);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#hashCode()
	 */
	@Override
	public int hashCode() {
		return 42;
	}

}
