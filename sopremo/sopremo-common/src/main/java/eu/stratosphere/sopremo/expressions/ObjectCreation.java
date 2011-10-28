package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
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
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			final ObjectNode objectNode = new ObjectNode();
			final Iterator<JsonNode> elements = ((ArrayNode) node).iterator();
			while (elements.hasNext()) {
				final JsonNode jsonNode = elements.next();
				if (!jsonNode.isNull())
					objectNode.putAll((ObjectNode) jsonNode);
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
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final ObjectCreation other = (ObjectCreation) obj;
		return this.mappings.equals(other.mappings);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
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
		int result = 1;
		result = prime * result + this.mappings.hashCode();
		return result;
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
	protected void toString(final StringBuilder builder) {
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
		protected void evaluate(final ObjectNode transformedNode, final JsonNode node, final EvaluationContext context) {
			final JsonNode exprNode = this.getExpression().evaluate(node, context);
			transformedNode.putAll((ObjectNode) exprNode);
		}

		@Override
		protected void toString(final StringBuilder builder) {
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
		public FieldAssignment(String target, EvaluationExpression expression) {
			super(target, expression);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -4873817871983692783L;

		@Override
		protected void evaluate(final ObjectNode transformedNode, final JsonNode node, final EvaluationContext context) {
			final JsonNode value = this.expression.evaluate(node, context);
			// if (!value.isNull())
			transformedNode.put(this.target, value);
		}
	}
	
	public static class TagMapping<Target> extends Mapping<Target> {
		/**
		 * Initializes TagMapping.
		 *
		 * @param target
		 * @param expression
		 */
		public TagMapping(Target target, EvaluationExpression expression) {
			super(target, expression);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -3919529819666259624L;

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping#evaluate(eu.stratosphere.sopremo.type.ObjectNode, eu.stratosphere.sopremo.type.JsonNode, eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		protected void evaluate(ObjectNode transformedNode, JsonNode node, EvaluationContext context) {
			throw new EvaluationException("Only tag mapping");
		}
	}

	public abstract static class Mapping<Target> implements SerializableSopremoType {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6372376844557378592L;

		protected final Target target;

		protected final EvaluationExpression expression;

		public Mapping(final Target target, final EvaluationExpression expression) {
			this.target = target;
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

		protected abstract void evaluate(final ObjectNode transformedNode, final JsonNode node,
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
		public String toString() {
			final StringBuilder builder = new StringBuilder();
			this.toString(builder);
			return builder.toString();
		}

		protected void toString(final StringBuilder builder) {
			builder.append(this.target).append("=");
			this.expression.toString(builder);
		}
	}

}
