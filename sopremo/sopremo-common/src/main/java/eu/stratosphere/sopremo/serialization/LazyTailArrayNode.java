package eu.stratosphere.sopremo.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.AbstractArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.util.AbstractIterator;
import eu.stratosphere.util.ConcatenatingIterator;

/**
 * <p>
 * This implementation builds on the fixed-size PactRecord. It needs therefore a {@link TailArraySchema}.Because it is
 * fixed-size, it has leading field, called "others", which is also an implementation of the IArrayNode interface. All
 * fields behind that are either <code>null</code> (when the tail is not filled completely yet) or with any JsonNode. If
 * the tail gets fully filled, each upcoming JsonNode, which gets added, goes into the "others" field.<br/>
 * So this is an abstraction of an array due to the PactRecord serialization.
 * </p>
 * *
 * <p>
 * Visualization with a tailSize of 5:<br/>
 * <ul>
 * <li>intern representation: <code>[[], null, null, null, IJsonNode, IJsonNode]</code></li>
 * <li>extern representation: <code>[IJsonNode, IJsonNode]</code></li>
 * </ul>
 * Tail filled:<br/>
 * <ul>
 * <li>intern representation: <code>[[IJsonNode, ...], IJsonNode, IJsonNode, IJsonNode, IJsonNode, IJsonNode]</code></li>
 * <li>extern representation: <code>[IJsonNode, ..., IJsonNode, IJsonNode, IJsonNode, IJsonNode, IJsonNode]</code></li>
 * </ul>
 * </p>
 * 
 * @author Michael Hopstock
 */
public class LazyTailArrayNode extends AbstractArrayNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -363746608697276853L;

	protected PactRecord record;

	protected TailArraySchema schema;

	public LazyTailArrayNode(final PactRecord record, final TailArraySchema schema) {
		this.record = record;
		this.schema = schema;
	}

	@Override
	public IArrayNode add(final IJsonNode node) {
		if (node == null)
			throw new NullPointerException();
		// we don't need to add nothing to the node
		if (node.isMissing())
			return this;

		// save last node in tail
		IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(this.schema.getTailSize(), JsonNodeWrapper.class));
		IJsonNode tmpNode;
		// replace with new node
		if (this.schema.getTailSize() > 0) {
			this.record.setField(this.schema.getTailSize(), SopremoUtil.wrap(node));
			// shift every node in tail and replace with oldNode
			for (int i = this.schema.getTailSize() - 1; i > 0; i--)
				if (this.record.isNull(i)) {
					this.record.setField(i, SopremoUtil.wrap(oldNode));
					// we found the beginning of the array, no need to go further
					return this;
				} else {
					tmpNode = SopremoUtil.unwrap(this.record.getField(i, JsonNodeWrapper.class));
					if (oldNode != null)
						this.record.setField(i, SopremoUtil.wrap(oldNode));
					oldNode = tmpNode;
				}
			// put first element of the tail into "others"
			if (oldNode != null)
				this.getOtherField().add(oldNode);

		} else
			this.getOtherField().add(node);
		return this;
	}

	@Override
	public IArrayNode add(final int index, final IJsonNode element) {
		if (element == null)
			throw new NullPointerException();

		if (element.isMissing())
			this.remove(index);

		if (index < 0 || index > this.size())
			throw new IndexOutOfBoundsException();

		// similar to adding nodes, insert it at the specific position and shift all elements before to the left
		final int recordPosition = this.schema.getTailSize() - this.size() + index;
		if (recordPosition < 0)
			this.getOtherField().add(index, element);
		else {
			IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(recordPosition + 1, JsonNodeWrapper.class));
			IJsonNode tmpNode;
			this.record.setField(recordPosition + 1, SopremoUtil.wrap(element));
			for (int i = recordPosition + 1; i > 0; i--)
				if (this.record.isNull(i)) {
					this.record.setField(i, SopremoUtil.wrap(oldNode));
					return this;
				} else {
					tmpNode = SopremoUtil.unwrap(this.record.getField(i, JsonNodeWrapper.class));
					if (oldNode != null)
						this.record.setField(i, SopremoUtil.wrap(oldNode));
					oldNode = tmpNode;
				}

			if (oldNode != null)
				this.getOtherField().add(oldNode);
		}

		return this;
	}

	@Override
	public void clear() {
		for (int i = 1; i <= this.schema.getTailSize(); i++)
			this.record.setNull(i);

		this.getOtherField().clear();
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
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
	public IJsonNode get(final int index) {
		final int size = this.size();
		if (index < 0 || index >= size)
			return MissingNode.getInstance();
		final int recordPosition = this.schema.getTailSize() - size + index;
		if (recordPosition >= 0)
			return SopremoUtil.unwrap(this.record.getField(recordPosition + 1,
				JsonNodeWrapper.class));
		else
			return this.getOtherField().get(index);
	}

	@Override
	public PactRecord getJavaValue() {
		return this.record;
	}

	/**
	 * Returns the arrayNode "others", which is the first in the PactRecord before the tail starts.
	 * 
	 * @return the field "others" of the PactRecord
	 */
	public IArrayNode getOtherField() {
		return (IArrayNode) SopremoUtil.unwrap(this.record.getField(0,
			JsonNodeWrapper.class));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.schema.getTailSize() == 0 ? this.getOtherField().isEmpty() : this.record.isNull(0);
	}

	@Override
	public Iterator<IJsonNode> iterator() {
		final Iterator<IJsonNode> othersIterator = this.getOtherField().iterator();
		final Iterator<IJsonNode> tailIterator = new FixedIndexIterator(1, this.schema.getTailSize() + 1);

		return new ConcatenatingIterator<IJsonNode>(othersIterator, tailIterator);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		throw new UnsupportedOperationException("Use other ArrayNode Implementation instead");
	}

	@Override
	public IJsonNode remove(final int index) {
		if (index < 0 || index >= this.size())
			return MissingNode.getInstance();

		final int size = this.size();
		final int recordPosition = this.schema.getTailSize() - size + index;
		if (recordPosition < 0)
			return this.getOtherField().remove(index);
		else {
			// save the element which gets removed to return it later
			final IJsonNode oldNode = SopremoUtil.unwrap(this.record
				.getField(recordPosition + 1, JsonNodeWrapper.class));
			// shift every node before one time to the right
			for (int i = recordPosition; i > 0; i--)
				if (!this.record.isNull(i))
					this.record.setField(i + 1, this.record.getField(i, JsonNodeWrapper.class));
				else {
					this.record.setNull(i + 1);
					// we are in front of the first element, no need go further
					return oldNode;
				}
			if (size > this.schema.getTailSize())
				// remove last element from "others" and put it into first field of the tail
				this.record.setField(1, this.getOtherField().remove(size - this.schema.getTailSize()));
			else
				// no elements in "others", set first field of the tail null
				this.record.setNull(1);
			return oldNode;
		}
	}

	@Override
	public IJsonNode set(final int index, final IJsonNode node) {
		if (node == null)
			throw new NullPointerException();

		if (node.isMissing())
			return this.remove(index);

		if (index < 0 || index >= this.size())
			if (index == this.size()) {
				this.add(node);
				return MissingNode.getInstance();
			} else
				throw new IndexOutOfBoundsException();
		final int recordPosition = this.schema.getTailSize() - this.size() + index;
		if (recordPosition < 0)
			return this.getOtherField().set(index, node);
		else {
			// save node for return
			final IJsonNode oldNode = SopremoUtil.unwrap(this.record.getField(recordPosition + 1,
				JsonNodeWrapper.class));
			// replace it
			this.record.setField(recordPosition + 1, node);
			return oldNode;
		}
	}

	@Override
	public int size() {
		final IArrayNode others = this.getOtherField();
		// we have to manually iterate over our record to get his size
		// because there is a difference between NullNode and MissingNode
		int count = 0;
		for (int i = 1; i <= this.schema.getTailSize(); i++)
			if (!this.record.isNull(i))
				count++;
		return count + others.size();
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
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

	@Override
	public void write(final DataOutput out) throws IOException {
		throw new UnsupportedOperationException("Use other ArrayNode Implementation instead");
	}

	@Override
	public IArrayNode addAll(final Collection<? extends IJsonNode> c) {
		for (final IJsonNode node : c)
			this.add(node);

		return this;
	}

	@Override
	public IArrayNode addAll(final IArrayNode arraynode) {
		for (final IJsonNode node : arraynode)
			this.add(node);

		return this;
	}

	@Override
	public IJsonNode[] toArray() {
		final IJsonNode[] result = new IJsonNode[this.size()];
		int i = 0;
		for (final IJsonNode node : this)
			result[i++] = node;

		return result;
	}

	@Override
	public IArrayNode addAll(final IJsonNode[] nodes) {
		for (final IJsonNode node : nodes)
			this.add(node);
		return this;
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return 0;
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		throw new UnsupportedOperationException("Use other ArrayNode Implementation instead");
	}

	/**
	 * @author Michael Hopstock
	 */
	private final class FixedIndexIterator extends AbstractIterator<IJsonNode> {
		int lastIndex = 0;

		int endIndex = 0;

		/**
		 * Initializes LazyArrayNode.FixedIndexIterator.
		 */
		public FixedIndexIterator(final int startIndex, final int endIndex) {
			this.lastIndex = startIndex;
			this.endIndex = endIndex;
		}

		@Override
		protected IJsonNode loadNext() {
			while (this.lastIndex < this.endIndex) {
				final IJsonNode value = SopremoUtil.unwrap(LazyTailArrayNode.this.record.getField(this.lastIndex,
					JsonNodeWrapper.class));
				this.lastIndex++;
				if (value != null)
					return value;
			}
			return this.noMoreElements();

		}
	}
}
