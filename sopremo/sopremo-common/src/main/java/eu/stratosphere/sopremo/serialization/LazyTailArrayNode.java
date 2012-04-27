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

public class LazyTailArrayNode extends JsonNode implements IArrayNode {

	/**
	 * @author Michael Hopstock
	 */
	private final class FixedIndexIterator extends AbstractIterator<IJsonNode> {
		int lastIndex = 0;

		int endIndex = 0;

		/**
		 * Initializes LazyArrayNode.FixedIndexIterator.
		 */
		public FixedIndexIterator(int startIndex, int endIndex) {
			this.lastIndex = startIndex;
			this.endIndex = endIndex;
		}

		@Override
		protected IJsonNode loadNext() {
			if (!LazyTailArrayNode.this.record.isNull(lastIndex) && lastIndex < this.endIndex) {
				IJsonNode value = SopremoUtil.unwrap(LazyTailArrayNode.this.record.getField(this.lastIndex,
					JsonNodeWrapper.class));
				this.lastIndex++;
				return value;
			}
			return noMoreElements();

		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -363746608697276853L;

	protected PactRecord record;

	protected TailArraySchema schema;

	public LazyTailArrayNode(PactRecord record, TailArraySchema schema) {
		this.record = record;
		this.schema = schema;
	}

	@Override
	public Iterator<IJsonNode> iterator() {
		Iterator<IJsonNode> othersIterator = this.getOtherField().iterator();
		Iterator<IJsonNode> tailIterator = new FixedIndexIterator(1, this.schema.getTailSize() + 1);

		return new ConcatenatingIterator<IJsonNode>(othersIterator, tailIterator);
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
		final LazyTailArrayNode node = (LazyTailArrayNode) other;
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

	/**
	 * Returns the arrayNode "others", which is in between the head and the tail.
	 * 
	 * @return the field "others" of the PactRecord
	 */
	public IArrayNode getOtherField() {
		return (IArrayNode) SopremoUtil.unwrap(this.record.getField(0,
			JsonNodeWrapper.class));
	}

	@Override
	public int size() {
		final IArrayNode others = this.getOtherField();
		// we have to manually iterate over our record to get his size
		// because there is a difference between NullNode and MissingNode
		int count = 0;
		for (int i = 1; i <= this.schema.getTailSize(); i++) {
			if (!this.record.isNull(i)) {
				count++;
			} else
				return count;
		}
		return count + others.size();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.schema.getTailSize() == 0 ? getOtherField().isEmpty() : this.record.isNull(0);
	}
	
	@Override
	public IArrayNode add(IJsonNode node) {
		// TODO implement new ArraySchema with tail
		if (node == null) {
			throw new NullPointerException();
		}
		// we don't need to add nothing to the node
		if (node.isMissing()) {
			return this;
		}
		// first try to fill the tail

		// save last node in tail
		IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(this.schema.getTailSize(), JsonNodeWrapper.class));
		IJsonNode tmpNode;
		// replace with new node
		this.record.setField(this.schema.getTailSize(), SopremoUtil.wrap(node));
		// shift every node in tail and replace with oldNode
		for (int i = this.schema.getTailSize() - 1; i > 0; i--) {
			if (this.record.isNull(i)) {
				this.record.setField(i, SopremoUtil.wrap(oldNode));
				return this;
			} else {
				tmpNode = SopremoUtil.unwrap(this.record.getField(i, JsonNodeWrapper.class));
				this.record.setField(i, SopremoUtil.wrap(oldNode));
				oldNode = tmpNode;
			}
		}

		this.getOtherField().add(node);
		return this;
	}

	@Override
	public IArrayNode add(int index, IJsonNode element) {
		// TODO implement new ArraySchema with tail
		if (element == null) {
			throw new NullPointerException();
		}

		if (element.isMissing()) {
			this.remove(index);
		}

		if (index < 0 || index > this.size()) {
			throw new IndexOutOfBoundsException();
		}

		if (index < this.schema.getTailSize()) {
			for (int i = this.schema.getTailSize() - 1; i >= index; i--) {
				if (!this.record.isNull(i)) {
					if (i == this.schema.getTailSize() - 1) {
						this.getOtherField().add(0, SopremoUtil.unwrap(this.record.getField(i, JsonNodeWrapper.class)));
					} else {
						this.record.setField(i + 1, this.record.getField(i, JsonNodeWrapper.class));
					}
				}
			}
			this.record.setField(index, SopremoUtil.wrap(element));
		}

		return this;
	}

	@Override
	public IJsonNode get(int index) {
		// TODO implement new ArraySchema with tail
		int size = this.size();
		if (index < 0 || index >= size) {
			return MissingNode.getInstance();
		}

		if (size <= this.schema.getTailSize()) {
			return SopremoUtil.unwrap(this.record.getField(this.schema.getTailSize() -size + index + 1, JsonNodeWrapper.class));
			
		} else {
			return this.getOtherField().get(index);
			
		}
	}

	@Override
	public IJsonNode set(int index, IJsonNode node) {
		// TODO implement new ArraySchema with tail
		if (node == null) {
			throw new NullPointerException();
		}

		if (node.isMissing()) {
			return this.remove(index);
		}

		if (index < 0 || index >= this.size()) {
			if (index == this.size()) {
				this.add(node);
				return MissingNode.getInstance();
			} else {
				throw new IndexOutOfBoundsException();
			}
		}

		if (index < this.schema.getTailSize()) {
			for (int i = 0; i < index; i++) {
				if (this.record.isNull(i))
					throw new IndexOutOfBoundsException();
			}
			IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
			this.record.setField(index, node);
			return oldNode;
		} else {
			return this.getOtherField().set(index - this.schema.getTailSize(), node);
		}
	}

	@Override
	public IJsonNode remove(int index) {
		// TODO implement new ArraySchema with tail
		if (index < 0 || index >= this.size()) {
			return MissingNode.getInstance();
		}

		if (index < this.schema.getTailSize()) {
			IJsonNode oldNode = SopremoUtil.wrap(this.getOtherField().remove(0));
			IJsonNode buffer;

			for (int i = this.schema.getTailSize() - 1; i >= index; i--) {
				buffer = this.record.getField(i, JsonNodeWrapper.class);
				if (buffer == null) {
					buffer = MissingNode.getInstance();
				}
				if (oldNode.isMissing()) {
					this.record.setNull(i);
				} else {
					this.record.setField(i, oldNode);
				}
				oldNode = buffer;
			}
			return SopremoUtil.unwrap(oldNode);

		} else {
			return this.getOtherField().remove(index - this.schema.getTailSize());
		}
	}

	@Override
	public void clear() {
		// TODO implement new ArraySchema with tail
		for (int i = 1; i <= this.schema.getTailSize(); i++) {
			this.record.setNull(i);
		}

		this.getOtherField().clear();
	}

	@Override
	public IArrayNode addAll(Collection<? extends IJsonNode> c) {
		for (IJsonNode node : c) {
			this.add(node);
		}

		return this;
	}

	@Override
	public IArrayNode addAll(IArrayNode arraynode) {
		for (IJsonNode node : arraynode) {
			this.add(node);
		}

		return this;
	}

	@Override
	public IJsonNode[] toArray() {
		IJsonNode[] result = new IJsonNode[this.size()];
		int i = 0;
		for (IJsonNode node : this) {
			result[i++] = node;
		}

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
