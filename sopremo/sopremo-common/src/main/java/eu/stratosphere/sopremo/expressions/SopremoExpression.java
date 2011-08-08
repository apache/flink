package eu.stratosphere.sopremo.expressions;

import java.util.Collections;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.function.FunctionRegistry;

public abstract class SopremoExpression<ContextType extends EvaluationContext> implements SerializableSopremoType,
		Iterable<SopremoExpression<ContextType>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8127381947526245461L;

	/**
	 * Evaluates the given node in the provided context.<br>
	 * The given node can either be a normal {@link JsonNode} or one of the following special nodes:
	 * <ul>
	 * <li>{@link CompactArrayNode} wrapping an array of nodes if the evaluation is performed for more than one
	 * {@link JsonStream},
	 * <li>{@link StreamArrayNode} wrapping an iterator of incoming nodes which is most likely the content of a complete
	 * {@link JsonStream} that is going to be aggregated, or
	 * <li>CompactArrayNode of StreamArrayNodes when aggregating multiple JsonStreams.
	 * </ul>
	 * <br>
	 * Consequently, the result may also be of one of the previously mentioned types.<br>
	 * The ContextType provides additional information that is relevant for the evaluation, for instance all registered
	 * functions in the {@link FunctionRegistry}.
	 * 
	 * @param node
	 *        the node that should be evaluated or a special node representing containing several nodes
	 * @param context
	 *        the context in which the node should be evaluated
	 * @return the node resulting from the evaluation or several nodes wrapped in a special node type
	 */
	public abstract JsonNode evaluate(JsonNode node, ContextType context);

	/**
	 * Appends a string representation of this expression to the builder. The method should return the same result as
	 * {@link #toString()} but provides a better performance when a string is composed of several child expressions.
	 * 
	 * @param builder
	 *        the builder to append to
	 */
	protected void toString(final StringBuilder builder) {
	}

	@SuppressWarnings("rawtypes")
	private final static Iterator EMPTY_ITERATOR = Collections.EMPTY_LIST.iterator();

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<SopremoExpression<ContextType>> iterator() {
		return EMPTY_ITERATOR;
	}
}
