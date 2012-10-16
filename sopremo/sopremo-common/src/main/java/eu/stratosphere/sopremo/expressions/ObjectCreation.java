package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Creates an object with the given {@link Mapping}s.
 */
@OptimizerHints(scope = Scope.ANY)
public class ObjectCreation extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5688226000742970692L;

	/**
	 * An ObjectCreation which copies the fields of all given {IObjectNode}s into a single node.
	 */
	public static final ObjectCreation CONCATENATION = new ObjectCreation() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5274811723343043990L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
			final ObjectNode targetObject = SopremoUtil.reinitializeTarget(target, ObjectNode.class);
			for (final IJsonNode jsonNode : (IArrayNode) node)
				if (!jsonNode.isNull())
					targetObject.putAll((IObjectNode) jsonNode);
			return targetObject;
		}
	};

	private List<Mapping<?>> mappings;

	/**
	 * Initializes an ObjectCreation with empty mappings.
	 */
	public ObjectCreation() {
		this(new ArrayList<Mapping<?>>());
	}

	/**
	 * Initializes an ObjectCreation with the given {@link Mapping}s.
	 * 
	 * @param mappings
	 *        the mappings that should be used
	 */
	public ObjectCreation(final List<Mapping<?>> mappings) {
		this.mappings = mappings;
	}

	/**
	 * Initializes an ObjectCreation with the given {@link FieldAssignment}s.
	 * 
	 * @param mappings
	 *        the assignments that should be used
	 */
	public ObjectCreation(final FieldAssignment... mappings) {
		this.mappings = new ArrayList<Mapping<?>>(Arrays.asList(mappings));
	}

	/**
	 * Adds a new {@link Mapping}
	 * 
	 * @param mapping
	 *        the new mapping
	 */
	public ObjectCreation addMapping(final Mapping<?> mapping) {
		if(mapping == null)
			throw new NullPointerException();
		this.mappings.add(mapping);
		return this;
	}

	/**
	 * Creates a new {@link FieldAssignment} and adds it to this expressions mappings.
	 * 
	 * @param target
	 *        the fieldname
	 * @param expression
	 *        the expression that should be used for the created FieldAssignemt
	 */
	public ObjectCreation addMapping(final String target, final EvaluationExpression expression) {
		if(target == null || expression == null)
			throw new NullPointerException();
		this.mappings.add(new FieldAssignment(target, expression));
		return this;
	}

	/**
	 * Creates a new {@link ExpressionAssignment} and adds it to this expressions mappings.
	 * 
	 * @param target
	 *        the expression that specifies the target location
	 * @param expression
	 *        the expression that should be used for the created FieldAssignemt
	 */
	public ObjectCreation addMapping(final EvaluationExpression target, final EvaluationExpression expression) {
		if(target == null || expression == null)
			throw new NullPointerException();
		this.mappings.add(new ExpressionAssignment(target, expression));
		return this;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ObjectCreation other = (ObjectCreation) obj;
		return this.mappings.equals(other.mappings);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		final ObjectNode targetObject = SopremoUtil.reinitializeTarget(target, ObjectNode.class);
		for (final Mapping<?> mapping : this.mappings)
			mapping.evaluate(node, targetObject, context);
		return targetObject;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#clone()
	 */
	@Override
	public ObjectCreation clone() {
		final ObjectCreation clone = (ObjectCreation) super.clone();
		clone.mappings = new ArrayList<ObjectCreation.Mapping<?>>(this.mappings.size());
		for (Mapping<?> mapping : this.mappings) 
			clone.mappings.add(mapping.clone());
		return clone;
	}
	
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.ContainerExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EvaluationExpression transformRecursively(final TransformFunction function) {
		for (final Mapping mapping : this.mappings) {
			mapping.expression = mapping.expression.transformRecursively(function);
			if (mapping.target instanceof EvaluationExpression)
				mapping.target = ((EvaluationExpression) mapping.target).transformRecursively(function);
		}
		return function.call(this);
	}

	/**
	 * Returns the mapping at the specified index
	 * 
	 * @param index
	 *        the index of the mapping that should be returned
	 * @return the mapping at the specified index
	 */
	public Mapping<?> getMapping(final int index) {
		return this.mappings.get(index);
	}

	/**
	 * Returns the mappings
	 * 
	 * @return the mappings
	 */
	public List<Mapping<?>> getMappings() {
		return this.mappings;
	}

	/**
	 * Returns how many mappings are specified
	 * 
	 * @return the mapping count
	 */
	public int getMappingSize() {
		return this.mappings.size();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.mappings.hashCode();
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("{");
		final Iterator<Mapping<?>> mappingIterator = this.mappings.iterator();
		while (mappingIterator.hasNext()) {
			final Mapping<?> entry = mappingIterator.next();
			entry.toString(builder);
			if (mappingIterator.hasNext())
				builder.append(", ");
		}
		builder.append("}");
	}

	/**
	 * An ObjectCreation which copies the fields of an {@link IObjectNode}.
	 */
	public static class CopyFields extends FieldAssignment {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8809405108852546800L;

		public CopyFields(final EvaluationExpression expression) {
			super("*", expression);
		}

		@Override
		protected void evaluate(final IJsonNode node, final IObjectNode target, final EvaluationContext context) {
			final IJsonNode exprNode = this.getExpression().evaluate(node, null, context);
			target.putAll((IObjectNode) exprNode);
		}

		@Override
		public void toString(final StringBuilder builder) {
			this.getExpression().toString(builder);
			builder.append(".*");
		}
	}

	public static class FieldAssignment extends Mapping<String> {
		/**
		 * Initializes FieldAssignment.
		 * 
		 * @param target
		 *        the fieldname
		 * @param expression
		 *        the expression that evaluates to this fields value
		 */
		public FieldAssignment(final String target, final EvaluationExpression expression) {
			super(target, expression);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -4873817871983692783L;

		@Override
		protected void evaluate(final IJsonNode node, final IObjectNode target, final EvaluationContext context) {
			final IJsonNode value = this.expression.evaluate(node, target.get(this.target), context);
			// if (!value.isNull())
			target.put(this.target, value);
		}
	}

	public static class TagMapping extends Mapping<EvaluationExpression> {
		/**
		 * Initializes TagMapping.
		 * 
		 * @param target
		 * @param expression
		 */
		public TagMapping(final EvaluationExpression target, final EvaluationExpression expression) {
			super(target, expression);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -3919529819666259624L;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping#evaluate(eu.stratosphere.sopremo.type.ObjectNode,
		 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		protected void evaluate(final IJsonNode node, final IObjectNode target, final EvaluationContext context) {
			throw new EvaluationException("Only tag mapping");
		}
	}

	public static class ExpressionAssignment extends Mapping<EvaluationExpression> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3767842798206835622L;

		public ExpressionAssignment(final EvaluationExpression target, final EvaluationExpression expression) {
			super(target, expression);
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping#clone()
		 */
		@Override
		protected ExpressionAssignment clone() {
			final ExpressionAssignment clone = (ExpressionAssignment) super.clone();
			clone.target = clone.target.clone();
			return clone;
		}
		
		private IJsonNode lastResult;

		@Override
		protected void evaluate(final IJsonNode node, final IObjectNode target, final EvaluationContext context) {
			this.lastResult = this.expression.evaluate(node, this.lastResult, context);
			this.target.set(target, this.lastResult, context);
		}
	}

	public abstract static class Mapping<Target> extends AbstractSopremoType implements ISerializableSopremoType, Cloneable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6372376844557378592L;

		protected Target target;

		protected EvaluationExpression expression;

		/**
		 * Initializes Mapping with the given {@link Target} and the given {@link EvaluationExpression}.
		 * 
		 * @param target
		 *        the target of this mapping
		 * @param expression
		 *        the expression that evaluates to this mappings value
		 */
		public Mapping(final Target target, final EvaluationExpression expression) {
			this.target = target;
			this.expression = expression;
		}

		/**
		 * Sets the expression to the specified value.
		 * 
		 * @param expression
		 *        the expression to set
		 */
		public void setExpression(final EvaluationExpression expression) {
			if (expression == null)
				throw new NullPointerException("expression must not be null");

			this.expression = expression;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#clone()
		 */
		@SuppressWarnings("unchecked")
		@Override
		protected Mapping<Target> clone() {
			try {
				final Mapping<Target> clone = (Mapping<Target>) super.clone();
				clone.expression = this.expression.clone();
				return clone;
			} catch (CloneNotSupportedException e) {
				return null;
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
			final Mapping<?> other = (Mapping<?>) obj;
			return this.target.equals(other.target) && this.expression.equals(other.expression);
		}

		protected abstract void evaluate(IJsonNode node, IObjectNode target,
				final EvaluationContext context);

		/**
		 * Returns the expression
		 * 
		 * @return the expression
		 */
		public EvaluationExpression getExpression() {
			return this.expression;
		}

		/**
		 * Returns the target
		 * 
		 * @return the target
		 */
		public Target getTarget() {
			return this.target;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.expression.hashCode();
			result = prime * result + this.target.hashCode();
			return result;
		}

		@Override
		public void toString(final StringBuilder builder) {
			builder.append(this.target).append("=");
			this.expression.toString(builder);
		}
	}

}
