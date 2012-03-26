package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Merges several arrays by taking the first non-null value for each respective array.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.ARRAY, transitive = true, minNodes = 1, maxNodes = OptimizerHints.UNBOUND, iterating = true)
public class ArrayMerger extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6884623565349727369L;

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		final Iterator<IJsonNode> arrays = ((ArrayNode) node).iterator();

		if (target == null || !(target instanceof IArrayNode)) {
			target = new ArrayNode();
		} else {
			((IArrayNode) target).clear();
		}
		IJsonNode nextNode;
		while (arrays.hasNext())
			if ((nextNode = arrays.next()) != NullNode.getInstance()) {

				final IArrayNode array = (IArrayNode) nextNode;
				for (int index = 0; index < array.size(); index++)
					if (((IArrayNode) target).size() <= index)
						((IArrayNode) target).add(array.get(index));
					else if (this.isNull(((IArrayNode) target).get(index)) && !this.isNull(array.get(index)))
						((IArrayNode) target).set(index, array.get(index));
			}
		return target;
	}

	private boolean isNull(final IJsonNode value) {
		return value == null || value.isNull();
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("[*]+...+[*]");
	}

}
