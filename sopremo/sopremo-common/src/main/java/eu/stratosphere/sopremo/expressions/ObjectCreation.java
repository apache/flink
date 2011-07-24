package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SerializableSopremoType;
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
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			ObjectNode objectNode = JsonUtil.NODE_FACTORY.objectNode();
			Iterator<JsonNode> elements = node.getElements();
			while (elements.hasNext()) {
				JsonNode jsonNode = elements.next();
				if (!jsonNode.isNull())
					objectNode.putAll((ObjectNode) jsonNode);
			}
			return objectNode;
		}
	};

	private List<Mapping> mappings;

	public ObjectCreation() {
		this(new ArrayList<Mapping>());
	}

	public ObjectCreation(List<Mapping> mappings) {
		this.mappings = mappings;
	}

	public ObjectCreation(Mapping... mappings) {
		this(Arrays.asList(mappings));
	}

	public void addMapping(Mapping mapping) {
		this.mappings.add(mapping);
	}

	public void addMapping(String target, EvaluationExpression expression) {
		this.mappings.add(new Mapping(target, expression));
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		ObjectCreation other = (ObjectCreation) obj;
		return this.mappings.equals(other.mappings);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		ObjectNode transformedNode = JsonUtil.OBJECT_MAPPER.createObjectNode();
		for (Mapping mapping : this.mappings)
			mapping.evaluate(transformedNode, node, context);
		return transformedNode;
	}

	public Mapping getMapping(int index) {
		return this.mappings.get(index);
	}

	public List<Mapping> getMappings() {
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
		return new ConversionIterator<Mapping, EvaluationExpression>(this.mappings.iterator()) {
			@Override
			protected EvaluationExpression convert(Mapping inputObject) {
				return inputObject.getExpression();
			}
		};
	}

	@Override
	public void replace(EvaluationExpression toReplace, EvaluationExpression replaceFragment) {
		for (Mapping mapping : this.mappings)
			if (mapping.getExpression() instanceof ContainerExpression)
				((ContainerExpression) mapping.getExpression()).replace(toReplace, replaceFragment);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append("{");
		Iterator<Mapping> mappingIterator = this.mappings.iterator();
		while (mappingIterator.hasNext()) {
			Mapping entry = mappingIterator.next();
			entry.toString(builder);
			if (mappingIterator.hasNext())
				builder.append(", ");
		}
		builder.append("}");
	}

	public static class CopyFields extends Mapping {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8809405108852546800L;

		public CopyFields(EvaluationExpression expression) {
			super("*", expression);
		}

		@Override
		protected void evaluate(ObjectNode transformedNode, JsonNode node, EvaluationContext context) {
			JsonNode exprNode = this.getExpression().evaluate(node, context);
			transformedNode.putAll((ObjectNode) exprNode);
		}

		@Override
		protected void toString(StringBuilder builder) {
			this.getExpression().toString(builder);
			builder.append(".*");
		}
	}

	public static class Mapping implements SerializableSopremoType {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6372376844557378592L;

		private final String target;

		private final EvaluationExpression expression;

		public Mapping(String target, EvaluationExpression expression) {
			this.target = target;
			this.expression = expression;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			Mapping other = (Mapping) obj;
			return this.target.equals(other.target) && this.expression.equals(other.expression);
		}

		protected void evaluate(ObjectNode transformedNode, JsonNode node, EvaluationContext context) {
			transformedNode.put(this.target, this.expression.evaluate(node, context));
		}

		public EvaluationExpression getExpression() {
			return this.expression;
		}

		public String getTarget() {
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
			StringBuilder builder = new StringBuilder();
			this.toString(builder);
			return builder.toString();
		}

		protected void toString(StringBuilder builder) {
			builder.append(this.target).append("=");
			this.expression.toString(builder);
		}
	}

}
