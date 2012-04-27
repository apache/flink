package eu.stratosphere.sopremo.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.util.AbstractIterator;
import eu.stratosphere.util.ConcatenatingIterator;

public class LazyHeadArrayNode extends JsonNode implements IArrayNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -363746608697276853L;

	protected PactRecord record;

	protected HeadArraySchema schema;

	public LazyHeadArrayNode(PactRecord record, HeadArraySchema schema) {
		this.record = record;
		this.schema = schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<IJsonNode> iterator() {
		Iterator<IJsonNode> iterator2 = this.getOtherField().iterator();
		Iterator<IJsonNode> iterator1 = new AbstractIterator<IJsonNode>() {

			int lastIndex = 0;

			@Override
			protected IJsonNode loadNext() {
				while (this.lastIndex < LazyHeadArrayNode.this.schema.getHeadSize()) {
					if (!LazyHeadArrayNode.this.record.isNull(lastIndex)) {
						IJsonNode value = SopremoUtil.unwrap(LazyHeadArrayNode.this.record.getField(this.lastIndex,
							JsonNodeWrapper.class));
						this.lastIndex++;
						return value;
					}

					return this.noMoreElements();
				}
				return this.noMoreElements();
			}

		};

		return new ConcatenatingIterator<IJsonNode>(iterator1, iterator2);
	}

	@Override
	public Type getType() {
		return Type.ArrayNode;
	}

	@Override
	public void read(DataInput in) throws IOException {
		throw new UnsupportedOperationException("Use other ArrayNode Implementation instead");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("Use other ArrayNode Implementation instead");
	}

	@Override
	public PactRecord getJavaValue() {
		return this.record;
	}

	@Override
	public int compareToSameType(IJsonNode other) {
		final LazyHeadArrayNode node = (LazyHeadArrayNode) other;
		final Iterator<IJsonNode> entries1 = this.iterator(), entries2 = node.iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final IJsonNode entry1 = entries1.next(), entry2 = entries2.next();
			final int comparison = entry1.compareTo(entry2);
			if (comparison != 0)
				return comparison;
		}

		if (!entries1.hasNext())
			return entries2.hasNext() ? -1 : 0;
		if (!entries2.hasNext())
			return 1;
		return 0;
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		sb.append('[');

		int count = 0;
		for (final IJsonNode node : this) {
			if (count > 0)
				sb.append(',');
			++count;

			node.toString(sb);
		}

		sb.append(']');
		return sb;
	}

	public IArrayNode getOtherField() {
		return (IArrayNode) SopremoUtil.unwrap(this.record.getField(this.schema.getHeadSize(),
			JsonNodeWrapper.class));
	}

	@Override
	public int size() {
		final IArrayNode others = this.getOtherField();
		// we have to manually iterate over our record to get his size
		// because there is a difference between NullNode and MissingNode
		int count = 0;
		for (int i = 0; i < this.schema.getHeadSize(); i++)
			if (!this.record.isNull(i))
				count++;
			else
				return count;
		return count + others.size();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.schema.getHeadSize() == 0 ? getOtherField().isEmpty() : this.record.isNull(0);
	}

	@Override
	public IArrayNode add(IJsonNode node) {
		if (node == null)
			throw new NullPointerException();

		for (int i = 0; i < this.schema.getHeadSize(); i++)
			if (this.record.isNull(i) && !node.isMissing()) {
				this.record.setField(i, SopremoUtil.wrap(node));
				return this;
			}

		this.getOtherField().add(node);
		return this;
	}

	@Override
	public IArrayNode add(int index, IJsonNode element) {
		if (element == null)
			throw new NullPointerException();

		if (element.isMissing())
			this.remove(index);

		if (index < 0 || index > this.size()) {
			throw new IndexOutOfBoundsException();
		}

		if (index < this.schema.getHeadSize()) {
			for (int i = this.schema.getHeadSize() - 1; i >= index; i--)
				if (!this.record.isNull(i))
					if (i == this.schema.getHeadSize() - 1)
						this.getOtherField().add(0, SopremoUtil.unwrap(this.record.getField(i, JsonNodeWrapper.class)));
					else
						this.record.setField(i + 1, this.record.getField(i, JsonNodeWrapper.class));
			this.record.setField(index, SopremoUtil.wrap(element));
		}

		return this;

		// // recursive insertion
		// if (!this.record.isNull(index)) {
		// IJsonNode oldNode = this.record.getField(index, JsonNodeWrapper.class);
		// this.record.setField(index, element);
		// this.add(index + 1, oldNode);
		// }
		// } else {
		// this.getOtherField().add(index - this.schema.getHeadSize(), element);
		// }
		//
		// return this;
	}

	@Override
	public IJsonNode get(int index) {
		if (index < 0 || index >= this.size())
			return MissingNode.getInstance();

		if (index < this.schema.getHeadSize())
			return SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
		return this.getOtherField().get(index - this.schema.getHeadSize());
	}

	@Override
	public IJsonNode set(int index, IJsonNode node) {
		if (node == null)
			throw new NullPointerException();

		if (node.isMissing())
			return this.remove(index);

		if (index < 0 || index >= this.size()) {
			if (index == this.size()) {
				this.add(node);
				return MissingNode.getInstance();
			}
			throw new IndexOutOfBoundsException();
		}

		if (index < this.schema.getHeadSize()) {
			for (int i = 0; i < index; i++)
				if (this.record.isNull(i))
					throw new IndexOutOfBoundsException();
			IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
			this.record.setField(index, node);
			return oldNode;
		}
		return this.getOtherField().set(index - this.schema.getHeadSize(), node);
	}

	@Override
	public IJsonNode remove(int index) {
		if (index < 0 || index >= this.size())
			return MissingNode.getInstance();

		if (index < this.schema.getHeadSize()) {
			IJsonNode oldNode = SopremoUtil.wrap(this.getOtherField().remove(0));
			IJsonNode buffer;

			for (int i = this.schema.getHeadSize() - 1; i >= index; i--) {
				buffer = this.record.getField(i, JsonNodeWrapper.class);
				if (buffer == null) {
					buffer = MissingNode.getInstance();
				}
				if (oldNode.isMissing()) 
					this.record.setNull(i);
				else
					this.record.setField(i, oldNode);
				oldNode = buffer;
			}
			return SopremoUtil.unwrap(oldNode);

		}
		return this.getOtherField().remove(index - this.schema.getHeadSize());
	}

	@Override
	public void clear() {
		for (int i = 0; i < this.schema.getHeadSize(); i++)
			this.record.setNull(i);

		this.getOtherField().clear();
	}

	@Override
	public IArrayNode addAll(Collection<? extends IJsonNode> c) {
		for (IJsonNode node : c)
			this.add(node);

		return this;
	}

	@Override
	public IArrayNode addAll(IArrayNode arraynode) {
		for (IJsonNode node : arraynode)
			this.add(node);

		return this;
	}

	@Override
	public IJsonNode[] toArray() {
		IJsonNode[] result = new IJsonNode[this.size()];
		int i = 0;
		for (IJsonNode node : this)
			result[i++] = node;

		return result;
	}

	@Override
	public IArrayNode addAll(IJsonNode[] nodes) {
		for (IJsonNode node : nodes) {
			this.add(node);
		}
		return this;
	}

}
