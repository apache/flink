package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.util.ConversionIterator;

@OptimizerHints(scope = Scope.ANY)
public class ObjectCreation extends ContainerExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5688226000742970692L;

	public static final ObjectCreation CONCATENATION = new ObjectCreation() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5274811723343043990L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
			final ObjectNode objectNode = new ObjectNode();
			final Iterator<IJsonNode> elements = ((ArrayNode) node).iterator();
			while (elements.hasNext()) {
				final IJsonNode jsonNode = elements.next();
				if (!jsonNode.isNull())
					objectNode.putAll((IObjectNode) jsonNode);
			}
			return objectNode;
		}
	};

	private final List<Mapping<?>> mappings;

	public ObjectCreation() {
		this(new ArrayList<Mapping<?>>());
	}

	public ObjectCreation(final List<Mapping<?>> mappings) {
		this.mappings = mappings;
	}

	public ObjectCreation(final FieldAssignment... mappings) {
		this.mappings = new ArrayList<Mapping<?>>(Arrays.asList(mappings));
	}

	public void addMapping(final Mapping<?> mapping) {
		this.mappings.add(mapping);
	}

	public void addMapping(final String target, final EvaluationExpression expression) {
		this.mappings.add(new FieldAssignment(target, expression));
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ObjectCreation other = (ObjectCreation) obj;
		return this.mappings.equals(other.mappings);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		final ObjectNode transformedNode = new ObjectNode();
		for (final Mapping<?> mapping : this.mappings)
			mapping.evaluate(transformedNode, node, context);
		return transformedNode;
	}

	public Mapping<?> getMapping(final int index) {
		return this.mappings.get(index);
	}

	public List<Mapping<?>> getMappings() {
		return this.mappings;
	}

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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<? extends EvaluationExpression> getChildren() {
		final ArrayList<EvaluationExpression> list = new ArrayList<EvaluationExpression>();
		for (final Mapping<?> mapping : this.mappings)
			list.add(mapping.getExpression());
		return list;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(final List<? extends EvaluationExpression> children) {
		if (this.mappings.size() != children.size())
			throw new IllegalArgumentException();

		for (int index = 0; index < children.size(); index++)
			this.mappings.get(index).setExpression(children.get(index));
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return new ConversionIterator<Mapping<?>, EvaluationExpression>(this.mappings.iterator()) {
			@Override
			protected EvaluationExpression convert(final Mapping<?> inputObject) {
				return inputObject.getExpression();
			}
		};
	}

	@Override
	public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		for (final Mapping<?> mapping : this.mappings)
			if (mapping.getExpression() instanceof ContainerExpression)
				((ContainerExpression) mapping.getExpression()).replace(toReplace, replaceFragment);
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

	public static class CopyFields extends FieldAssignment {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8809405108852546800L;

		public CopyFields(final EvaluationExpression expression) {
			super("*", expression);
		}

		@Override
		protected void evaluate(final IObjectNode transformedNode, final IJsonNode node, final EvaluationContext context) {
			final IJsonNode exprNode = this.getExpression().evaluate(node, null, context);
			transformedNode.putAll((IObjectNode) exprNode);
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
		 * @param expression
		 */
		public FieldAssignment(final String target, final EvaluationExpression expression) {
			super(target, expression);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -4873817871983692783L;

		@Override
		protected void evaluate(final IObjectNode transformedNode, final IJsonNode node, final EvaluationContext context) {
			final IJsonNode value = this.expression.evaluate(node, null, context);
			// if (!value.isNull())
			transformedNode.put(this.target, value);
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
		protected void evaluate(final IObjectNode transformedNode, final IJsonNode node, final EvaluationContext context) {
			throw new EvaluationException("Only tag mapping");
		}
	}

	public abstract static class Mapping<Target> extends AbstractSopremoType implements SerializableSopremoType {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6372376844557378592L;

		protected final Target target;

		protected EvaluationExpression expression;

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

		protected abstract void evaluate(final IObjectNode transformedNode, final IJsonNode node,
				final EvaluationContext context);

		public EvaluationExpression getExpression() {
			return this.expression;
		}

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
