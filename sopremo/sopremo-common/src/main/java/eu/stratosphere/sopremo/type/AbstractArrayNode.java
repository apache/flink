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
import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
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
		final IJsonNode[] result = new IJsonNode[this.size()];
		int i = 0;
		for (final IJsonNode node : this)
			result[i++] = node;

		return result;
	}
}