package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

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
public class ArrayAccess extends EvaluationExpression {

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
		if (!super.equals(obj))
			return false;
		final ArrayAccess other = (ArrayAccess) obj;
		return this.startIndex == other.startIndex && this.endIndex == other.endIndex;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		if (!node.isArray())
			return MissingNode.getInstance();

		final IArrayNode arrayNode = (IArrayNode) node;
		if (this.isSelectingAll()) {
			final IArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);
			targetArray.addAll(arrayNode);
			return targetArray;
		}
		final int size = arrayNode.size();
		if (this.isSelectingRange()) {
			final IArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);
			int index = this.resolveIndex(this.startIndex, size);
			final int endIndex = this.resolveIndex(this.endIndex, size);
			final int increment = index < endIndex ? 1 : -1;

			for (boolean moreElements = true; moreElements; index += increment) {
				targetArray.add(arrayNode.get(index));
				moreElements = index != endIndex;
			}
			return targetArray;
		}

		final IJsonNode value = arrayNode.get(this.resolveIndex(this.startIndex, size));
		return value == null ? NullNode.getInstance() : value;
	}

	/**
	 * Returns the endIndex.
	 * 
	 * @return the endIndex
	 */
	public int getEndIndex() {
		return this.endIndex;
	}

	/**
	 * Returns the startIndex.
	 * 
	 * @return the startIndex
	 */
	public int getStartIndex() {
		return this.startIndex;
	}

	@Override
	public int hashCode() {
		return (47 * super.hashCode() + this.startIndex) * 47 + this.endIndex;
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

	@Override
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		if (this.isSelectingAll())
			return value;
		final int size = ((IArrayNode) node).size();
		if (this.isSelectingRange()) {
			final IArrayNode arrayNode = (IArrayNode) node;
			int index = this.resolveIndex(this.startIndex, size), replaceIndex = 0;
			final int endIndex = this.resolveIndex(this.endIndex, size);

			final int increment = index < endIndex ? 1 : -1;

			for (boolean moreElements = true; moreElements; index += increment, replaceIndex++) {
				arrayNode.set(index, ((IArrayNode) node).get(replaceIndex));
				moreElements = index != endIndex;
			}
		} else
			((IArrayNode) node).set(this.resolveIndex(this.startIndex, size), value);
		return node;
	}

	@Override
	public void toString(final StringBuilder builder) {
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

	private int resolveIndex(final int index, final int size) {
		if (index < 0)
			return size + index;
		return index;
	}

	/**
	 * Returns an optimal expression that returns an array that aggregates the given indices.<br>
	 * Please note that the result of this expression is always an array in contrast to ArrayAccess with only one
	 * index.
	 * 
	 * @param indices
	 *        the indices in the original array that should be concatenated to a new array
	 * @return an optimal expression that evaluates to an array
	 */
	public static EvaluationExpression arrayWithIndices(final int... indices) {
		switch (indices.length) {
		case 0:
			return new ArrayCreation();
		case 1:
			return new ArrayCreation(new ArrayAccess(indices[0]));
		default:
			boolean monoton = true;
			final int step = indices[1] - indices[0];
			if (Math.abs(step) != 1)
				monoton = false;

			for (int index = 2; monoton && index < indices.length; index += step)
				monoton = indices[index] - indices[index - 1] == step;

			if (monoton)
				return new ArrayAccess(indices[0], indices[indices.length - 1]);

			final ArrayAccess[] accesses = new ArrayAccess[indices.length];
			for (int index = 0; index < indices.length; index++)
				accesses[index] = new ArrayAccess(indices[index]);
			return new ArrayCreation();
		}
	}
}