/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * Abstract class to provide basic implementations for array type nodes.
 * 
 * @author Arvid Heise
 */
public abstract class AbstractArrayNode extends AbstractJsonNode implements IArrayNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4121730074586897715L;

	/**
	 * Initializes AbstractArrayNode.
	 */
	public AbstractArrayNode() {
		super();
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.clear();
		for (final IJsonNode child : (IArrayNode) otherNode)
			this.add(child);
	}

	@Override
	public final boolean isArray() {
		return true;
	}

	@Override
	public IArrayNode addAll(final Collection<? extends IJsonNode> c) {
		for (final IJsonNode jsonNode : c)
			this.add(jsonNode);
		return this;
	}

	@Override
	public IArrayNode addAll(final IArrayNode node) {
		for (final IJsonNode n : node)
			this.add(n);
		return this;
	}

	@Override
	public IArrayNode addAll(final IJsonNode[] nodes) {
		this.addAll(Arrays.asList(nodes));
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#contains(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public boolean contains(IJsonNode node) {
		for (final IJsonNode element : this)
			if (node.equals(element))
				return true;
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#asCollection()
	 */
	@Override
	public Collection<IJsonNode> asCollection() {
		return new AbstractCollection<IJsonNode>() {
			@Override
			public Iterator<IJsonNode> iterator() {
				return this.iterator();
			}

			@Override
			public int size() {
				return AbstractArrayNode.this.size();
			}
		};
	}

	@Override
	public final Type getType() {
		return Type.ArrayNode;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++)
			this.add(SopremoUtil.deserializeNode(in));
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.size());

		for (final IJsonNode child : this)
			SopremoUtil.serializeNode(out, child);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IStreamArrayNode#getFirst()
	 */
	@Override
	public IJsonNode getFirst() {
		return get(0);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#setAll(eu.stratosphere.sopremo.type.IJsonNode[])
	 */
	@Override
	public void setAll(final IJsonNode[] nodes) {
		this.clear();
		for (final IJsonNode node : nodes)
			this.add(node);
	}

	@Override
	public IJsonNode[] toArray() {
		IJsonNode[] result = new IJsonNode[this.size()];
		fillArray(result);
		return result;
	}

	protected void fillArray(IJsonNode[] result) {
		int i = 0;
		for (final IJsonNode node : this)
			result[i++] = node;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		final IArrayNode node = (IArrayNode) other;
		final Iterator<IJsonNode> entries1 = this.iterator(), entries2 = node.iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final IJsonNode entry1 = entries1.next(), entry2 = entries2.next();
			final int comparison = entry1.compareTo(entry2);
			if (comparison != 0)
				return comparison;
		}

		if (entries1.hasNext())
			return 1;
		if (entries2.hasNext())
			return -1;
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 41;
		int hashCode = prime;
		for (IJsonNode node : this)
			hashCode = (hashCode + node.hashCode()) * prime;
		return prime;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final Iterator<IJsonNode> thisIter = iterator(), thatIter = ((Iterable<IJsonNode>) obj).iterator();
		while (thisIter.hasNext() && thatIter.hasNext())
			if (!thisIter.next().equals(thatIter.next()))
				return false;
		return thisIter.hasNext() == thatIter.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#toArray(eu.stratosphere.sopremo.type.IJsonNode[])
	 */
	@Override
	public IJsonNode[] toArray(IJsonNode[] array) {
		final int size = this.size();
		if (array.length != size)
			array = new IJsonNode[size];
		fillArray(array);
		return array;
	}
}