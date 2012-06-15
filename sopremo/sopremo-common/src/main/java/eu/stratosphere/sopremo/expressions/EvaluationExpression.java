package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.IsEqualPredicate;
import eu.stratosphere.util.IdentityList;
import eu.stratosphere.util.IdentitySet;
import eu.stratosphere.util.IsInstancePredicate;
import eu.stratosphere.util.IsSamePredicate;
import eu.stratosphere.util.Predicate;
import eu.stratosphere.util.Reference;

/**
 * Represents all evaluable expressions.
 */
public abstract class EvaluationExpression implements Iterable<EvaluationExpression>, SerializableSopremoType,
		Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	protected Class<? extends JsonNode> expectedTarget;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags;

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
		public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
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

	// TODO: move to constant expression
	public static final EvaluationExpression NULL = new ConstantExpression(NullNode.getInstance()) {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2375203649638430872L;

		private Object readResolve() {
			return EvaluationExpression.NULL;
		}
	};

	/**
	 * Initializes EvaluationExpression.
	 */
	public EvaluationExpression() {
		this.tags = new IdentitySet<ExpressionTag>();
	}

	public void addTag(final ExpressionTag tag) {
		this.tags.add(tag);
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.tags = new IdentitySet<ExpressionTag>();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public EvaluationExpression clone() {
		try {
			return (EvaluationExpression) super.clone();
		} catch (CloneNotSupportedException e) {
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
		final EvaluationExpression other = (EvaluationExpression) obj;

		// return this.tags.equals(other.tags);
		return this.hasAllSemanticTags(other) && other.hasAllSemanticTags(this);
	}

	protected boolean hasAllSemanticTags(final EvaluationExpression other) {
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic() && !other.tags.contains(tag))
				return false;
		return true;
	}

	@SuppressWarnings("unchecked")
	public <T extends EvaluationExpression> T find(final Class<T> evaluableClass) {
		final Reference<T> ref = new Reference<T>();
		transformRecursively(new TransformFunction() {
			@Override
			public EvaluationExpression call(EvaluationExpression evaluationExpression) {
				if (ref.getValue() == null && evaluableClass.isInstance(evaluationExpression))
					ref.setValue((T) evaluationExpression);
				return evaluationExpression;
			}
		});
		return ref.getValue();
	}
	
	public List<EvaluationExpression> findAll(final Predicate<? super EvaluationExpression> predicate) {
		final ArrayList<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();
		transformRecursively(new TransformFunction() {
			@Override
			public EvaluationExpression call(EvaluationExpression evaluationExpression) {
				if (predicate.isTrue(evaluationExpression))
					expressions.add(evaluationExpression);
				return evaluationExpression;
			}
		});
		return expressions;
	}

	/**
	 * Recursively invokes the transformation function on all children and on the expression itself.<br>
	 * In general, this method modifies this expression.<br>
	 * To retain the original expression, next to the transformed expression, use {@link #clone()}.
	 * 
	 * @param function
	 *        the transformation function
	 * @return the transformed expression
	 */
	public EvaluationExpression transformRecursively(TransformFunction function) {
		return function.call(this);
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
		return replace(replacePredicate, new TransformFunction() {
			@Override
			public EvaluationExpression call(EvaluationExpression argument) {
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
		return transformRecursively(new TransformFunction() {
			@Override
			public EvaluationExpression call(EvaluationExpression evaluationExpression) {
				return replacePredicate.isTrue(evaluationExpression) ? replaceFunction.call(evaluationExpression) : evaluationExpression;
			}
		});
	}

	/**
	 * Replaces all expressions that are equal to <code>toReplace</code> with the given <code>replaceFragment</code>
	 * .
	 * 
	 * @param toReplace
	 *        the expressions that should be replaced
	 * @param replaceFragment
	 *        the expression which should replace another one
	 * @return the expression with the replaces
	 */
	public EvaluationExpression replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		return replace(new IsEqualPredicate(toReplace), replaceFragment);
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
		// intermediate tag expression
		final EvaluationExpression REMOVED = new UnevaluableExpression("Removed value");

		// remove in three steps
		// 1. replace all removed expression with REMOVED
		// 2. remove all REMOVED in containers
		EvaluationExpression taggedValues = transformRecursively(new TransformFunction() {
			@Override
			public EvaluationExpression call(EvaluationExpression evaluationExpression) {
				if (predicate.isTrue(evaluationExpression))
					return REMOVED;
				if (evaluationExpression instanceof ContainerExpression) {
					List<EvaluationExpression> children = new IdentityList<EvaluationExpression>();
					children.addAll(((ContainerExpression) evaluationExpression).getChildren());
					children.removeAll(Arrays.asList(REMOVED));
					((ContainerExpression) evaluationExpression).setChildren(children);
				}
				return evaluationExpression;
			}
		});
		// 3. replace all other REMOVED with VALUE
		return taggedValues.replace(new IsSamePredicate(REMOVED), REMOVED);
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
		return remove(new IsEqualPredicate(expressionToRemove));
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
		return remove(new IsInstancePredicate(expressionType));
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
		final int prime = 31;
		int result = 1;
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic())
				result = prime * result + tag.hashCode();
		return result;
	}

	public boolean hasTag(final ExpressionTag tag) {
		return this.tags.contains(tag);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this).iterator();
	}

	public boolean removeTag(final ExpressionTag preserve) {
		return this.tags.remove(preserve);
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
		this.appendTags(builder);
	}

	protected void appendTags(final StringBuilder builder) {
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic())
				builder.append(tag).append(" ");
	}

	public EvaluationExpression withTag(final ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}

	public Set<ExpressionTag> getTags() {
		return this.tags;
	}
}
