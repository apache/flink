package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.expressions.TraverseFunction.TraverseDirection;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.IsEqualPredicate;
import eu.stratosphere.util.IsInstancePredicate;
import eu.stratosphere.util.Predicate;

/**
 * Represents all evaluable expressions.
 */
public abstract class EvaluationExpression implements ISerializableSopremoType, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	/**
	 * Represents an expression that returns the input node without any modifications. The constant is mostly used for
	 * {@link Operator}s that do not perform any transformation to the input, such as a filter operator.
	 */
	public static final EvaluationExpression VALUE = new SingletonExpression("<value>") {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6430819532311429108L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
			return node;
		}

		@Override
		public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
			return value;
		};

		@Override
		protected Object readResolve() {
			return VALUE;
		}
	};

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public EvaluationExpression clone() {
		try {
			return (EvaluationExpression) super.clone();
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException("Cannot occur");
		}
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return true;
	}

	public EvaluationExpression findFirst(final Predicate<? super EvaluationExpression> predicate) {
		if (predicate.isTrue(this))
			return this;
		if (this instanceof ExpressionParent)
			for (EvaluationExpression child : ((ExpressionParent) this)) {
				final EvaluationExpression expr = child.findFirst(predicate);
				if (expr != null)
					return child.findFirst(predicate);
			}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T extends EvaluationExpression> T findFirst(final Class<T> evaluableClass) {
		return (T) findFirst(new IsInstancePredicate(evaluableClass));
	}

	public List<EvaluationExpression> findAll(final Predicate<? super EvaluationExpression> predicate) {
		final ArrayList<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();
		findAll(predicate, expressions);
		return expressions;
	}

	private void findAll(final Predicate<? super EvaluationExpression> predicate,
			final ArrayList<EvaluationExpression> expressions) {
		if (predicate.isTrue(this))
			expressions.add(this);

		if (this instanceof ExpressionParent)
			for (EvaluationExpression child : ((ExpressionParent) this))
				child.findAll(predicate, expressions);
	}

	/**
	 * Recursively invokes the transformation function on all children and on the expression itself.<br>
	 * In general, this method can modify this expression in-place.<br>
	 * To retain the original expression, next to the transformed expression, use {@link #clone()}.
	 * 
	 * @param function
	 *        the transformation function
	 * @return the transformed expression
	 */
	public EvaluationExpression transformRecursively(final TransformFunction function) {
		if (this instanceof ExpressionParent) {
			final ChildIterator iterator = ((ExpressionParent) this).iterator();
			while (iterator.hasNext()) {
				EvaluationExpression evaluationExpression = iterator.next();
				iterator.set(evaluationExpression.transformRecursively(function));
			}
		}
		return function.call(this);
	}

	/**
	 * Recursively invokes the traverse function on all children and on the expression itself.<br>
	 * In general, this method can modify this expression in-place.<br>
	 * To retain the original expression, next to the transformed expression, use {@link #clone()}.
	 * 
	 * @param function
	 *        the transformation function
	 * @return the transformed expression
	 */
	public TraverseFunction.TraverseDirection traverseRecursively(final TraverseFunction function) {
		switch (function.call(this)) {
		case CONTINUE:
			if (this instanceof ExpressionParent)
				for (EvaluationExpression child : ((ExpressionParent) this))
					if (child.traverseRecursively(function) == TraverseFunction.TraverseDirection.TERMINATE)
						return TraverseFunction.TraverseDirection.TERMINATE;
			// fall through
		case SKIP_CHILDREN:
			return TraverseDirection.CONTINUE;
		default:
			return TraverseDirection.TERMINATE;
		}
	}

	/**
	 * Replaces all expressions that satisfy the <code>replacePredicate</code> with the given
	 * <code>replaceFragment</code> .
	 * 
	 * @param replacePredicate
	 *        the predicate that indicates whether an expression should be replaced
	 * @param replaceFragment
	 *        the expression which should replace another one
	 * @return the expression with the replaces
	 */
	public EvaluationExpression replace(final Predicate<? super EvaluationExpression> replacePredicate,
			final EvaluationExpression replaceFragment) {
		return this.replace(replacePredicate, new TransformFunction() {
			@Override
			public EvaluationExpression call(final EvaluationExpression argument) {
				return replaceFragment;
			}
		});
	}

	/**
	 * Replaces all expressions that satisfy the <code>replacePredicate</code> with the given
	 * <code>replaceFunction</code> .
	 * 
	 * @param replacePredicate
	 *        the predicate that indicates whether an expression should be replaced
	 * @param replaceFunction
	 *        the function that is used to replace an expression
	 * @return the expression with the replaces
	 */
	public EvaluationExpression replace(final Predicate<? super EvaluationExpression> replacePredicate,
			final TransformFunction replaceFunction) {
		return this.transformRecursively(new TransformFunction() {
			@Override
			public EvaluationExpression call(final EvaluationExpression evaluationExpression) {
				return replacePredicate.isTrue(evaluationExpression) ? replaceFunction.call(evaluationExpression)
					: evaluationExpression;
			}
		});
	}

	/**
	 * Replaces all expressions that are equal to <code>toReplace</code> with the given <code>replaceFragment</code> .
	 * 
	 * @param toReplace
	 *        the expressions that should be replaced
	 * @param replaceFragment
	 *        the expression which should replace another one
	 * @return the expression with the replaces
	 */
	public EvaluationExpression replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		return this.replace(new IsEqualPredicate(toReplace), replaceFragment);
	}

	/**
	 * Removes all expression that satisfy the predicate.<br>
	 * If expressions cannot be completely removed, they are replaced by {@link EvaluationExpression#VALUE}.
	 * 
	 * @param predicate
	 *        the predicate that determines whether to remove an expression
	 * @return the expression without removed sub-expressions
	 */
	public EvaluationExpression remove(final Predicate<? super EvaluationExpression> predicate) {
		if (predicate.isTrue(this))
			return VALUE;

		if (this instanceof ExpressionParent)
			removeRecursively((ExpressionParent) this, predicate);
		return this;
	}

	private void removeRecursively(final ExpressionParent expressionParent,
			final Predicate<? super EvaluationExpression> predicate) {
		final ChildIterator iterator = expressionParent.iterator();
		while (iterator.hasNext()) {
			EvaluationExpression child = iterator.next();
			if (predicate.isTrue(child)) {
				if (!iterator.canChildrenBeRemoved())
					iterator.set(VALUE);
				else
					iterator.remove();
			} else if (child instanceof ExpressionParent)
				child.removeRecursively((ExpressionParent) this, predicate);
		}
	}

	/**
	 * Removes all expression that are equal to the given expression.<br>
	 * If expressions cannot be completely removed, they are replaced by {@link EvaluationExpression#VALUE}.
	 * 
	 * @param expressionToRemove
	 *        the expression to compare to
	 * @return the expression without removed sub-expressions
	 */
	public EvaluationExpression remove(final EvaluationExpression expressionToRemove) {
		return this.remove(new IsEqualPredicate(expressionToRemove));
	}

	/**
	 * Removes all expression that are from the given expression type.<br>
	 * If expressions cannot be completely removed, they are replaced by {@link EvaluationExpression#VALUE}.
	 * 
	 * @param expressionType
	 *        the expression type to remove
	 * @return the expression without removed sub-expressions
	 */
	public EvaluationExpression remove(final Class<?> expressionType) {
		return this.remove(new IsInstancePredicate(expressionType));
	}

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
	 * @param target
	 *        the target that should be used
	 * @param context
	 *        the context in which the node should be evaluated
	 * @return the node resulting from the evaluation or several nodes wrapped in a special node type
	 */
	public abstract IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context);

	@Override
	public int hashCode() {
		return 37;
	}

	/**
	 * Sets the value of the node specified by this expression using the given {@link EvaluationContext}.
	 * 
	 * @param node
	 *        the node to change
	 * @param value
	 *        the value to set
	 * @param context
	 *        the current <code>EvaluationContext</code>
	 * @return the node or a new node if the expression directly accesses the node
	 */
	@SuppressWarnings("unused")
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		throw new UnsupportedOperationException(String.format(
			"Cannot change the value with expression %s of node %s to %s", this, node, value));
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	/**
	 * Appends a string representation of this expression to the builder. The method should return the same result as
	 * {@link #toString()} but provides a better performance when a string is composed of several child expressions.
	 * 
	 * @param builder
	 *        the builder to append to
	 */
	@Override
	public void toString(final StringBuilder builder) {
	}

	protected void appendChildExpressions(final StringBuilder builder,
			final List<? extends EvaluationExpression> children, final String separator) {
		for (int index = 0; index < children.size(); index++) {
			children.get(index).toString(builder);
			if (index < children.size() - 1)
				builder.append(separator);
		}
	}
}
