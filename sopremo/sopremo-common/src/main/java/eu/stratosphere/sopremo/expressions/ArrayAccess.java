package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;

/**
 * Returns one or more elements of an array.<br>
 * There are two special cases supported when specifying the indices.
 * <ul>
 * <li>When one or both indices are negatives, the position is counted from the rear of the list. More specifically, the
 * index will be added to the size of the array.
 * <li>If the first index is higher than the second index, the returned list will still contain elements within the
 * range but in reversed order.
 * </ul>
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.ARRAY, iterating = true)
public class ArrayAccess extends EvaluationExpression implements WritableEvaluable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2326222517008315722L;

	private final int startIndex, endIndex;

	/**
	 * Initializes ArrayAccess that reproduces any input array.
	 */
	public ArrayAccess() {
		this(0, -1);
	}

	/**
	 * Initializes ArrayAccess that selects one element at a given location. If the location is negative, it will be
	 * added to the size of the array to allow selection of rear elements of arrays with unknown size.
	 * 
	 * @param index
	 *        the index of the element
	 */
	public ArrayAccess(final int index) {
		this(index, index);
	}

	/**
	 * Initializes ArrayAccess to return a subarray ranging from the start to the end location. If a location is
	 * negative, it will be added to the size of the array to allow selection of rear elements of arrays with unknown
	 * size.
	 * 
	 * @param startIndex
	 *        the start index
	 * @param endIndex
	 *        the end index (inclusive)
	 */
	public ArrayAccess(final int startIndex, final int endIndex) {
		// if (0 <= startIndex && 0 <= endIndex && endIndex < startIndex)
		// throw new IllegalArgumentException("startIndex < endIndex");
		// if (startIndex < 0 && endIndex < 0 && startIndex < endIndex)
		// throw new IllegalArgumentException("negative endIndex < negative startIndex");
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.startIndex == ((ArrayAccess) obj).startIndex && this.endIndex == ((ArrayAccess) obj).endIndex;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (this.isSelectingAll())
			return node;
		final int size = node.size();
		if (this.isSelectingRange()) {
			final ArrayNode arrayNode = new ArrayNode(JsonUtil.NODE_FACTORY);
			int index = this.resolveIndex(this.startIndex, size);
			final int endIndex = this.resolveIndex(this.endIndex, size);
			final int increment = index < endIndex ? 1 : -1;

			for (boolean moreElements = true; moreElements; index += increment) {
				arrayNode.add(node.get(index));
				moreElements = index != endIndex;
			}
			return arrayNode;
		}
		JsonNode value = node.get(this.resolveIndex(this.startIndex, size));
		return value == null ? NullNode.getInstance() : value;
	}

	@Override
	public JsonNode set(JsonNode node, JsonNode value, EvaluationContext context) {
		if (this.isSelectingAll())
			return value;
		final int size = node.size();
		if (this.isSelectingRange()) {
			final ArrayNode arrayNode = (ArrayNode) node;
			int index = this.resolveIndex(this.startIndex, size), replaceIndex = 0;
			final int endIndex = this.resolveIndex(this.endIndex, size);

			final int increment = index < endIndex ? 1 : -1;

			for (boolean moreElements = true; moreElements; index += increment, replaceIndex++) {
				arrayNode.set(index, value.get(replaceIndex));
				moreElements = index != endIndex;
			}
		} else
			((ArrayNode) node).set(this.resolveIndex(this.startIndex, size), value);
		return node;
	}
	
	@Override
	public EvaluationExpression asExpression() {
		return this;
	}

	@Override
	public int hashCode() {
		return (47 + this.startIndex) * 47 + this.endIndex;
	}

	/**
	 * Returns true if any incoming array would be wholly reproduced.
	 * 
	 * @return true if any incoming array would be wholly reproduced
	 */
	public boolean isSelectingAll() {
		return this.startIndex == 0 && this.endIndex == -1;
	}

	/**
	 * Returns true if more than one element is selected.
	 * 
	 * @return true if more than one element is selected
	 */
	public boolean isSelectingRange() {
		return this.startIndex != this.endIndex;
	}

	private int resolveIndex(final int index, final int size) {
		if (index < 0)
			return size + index;
		return index;
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append('[');
		if (this.isSelectingAll())
			builder.append('*');
		else {
			builder.append(this.startIndex);
			if (this.startIndex != this.endIndex) {
				builder.append(':');
				builder.append(this.endIndex);
			}
		}
		builder.append(']');
	}
}