package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoType;

public class ObjectCreation extends ContainerExpression {
	public static final ObjectCreation CONCATENATION = new ObjectCreation() {
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			ObjectNode objectNode = NODE_FACTORY.objectNode();
			Iterator<JsonNode> elements = node.getElements();
			while (elements.hasNext()) {
				JsonNode jsonNode = elements.next();
				if (!jsonNode.isNull())
					objectNode.putAll((ObjectNode) jsonNode);
			}
			return objectNode;
		}
	};

	public static class Mapping implements SopremoType {
		private final String target;

		private final EvaluableExpression expression;

		public Mapping(String target, EvaluableExpression expression) {
			this.target = target;
			this.expression = expression;
		}

		public String getTarget() {
			return target;
		}

		public EvaluableExpression getExpression() {
			return expression;
		}

		protected void toString(StringBuilder builder) {
			builder.append(target).append("=");
			expression.toString(builder);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			toString(builder);
			return builder.toString();
		}

		protected void evaluate(ObjectNode transformedNode, JsonNode node, EvaluationContext context) {
			transformedNode.put(target, expression.evaluate(node, context));
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + expression.hashCode();
			result = prime * result + target.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Mapping other = (Mapping) obj;
			return target.equals(other.target) && expression.equals(other.expression);
		}
	}

	public static class CopyFields extends Mapping {
		public CopyFields(EvaluableExpression expression) {
			super("*", expression);
		}

		protected void toString(StringBuilder builder) {
			getExpression().toString(builder);
			builder.append(".*");
		}

		@Override
		protected void evaluate(ObjectNode transformedNode, JsonNode node, EvaluationContext context) {
			JsonNode exprNode = getExpression().evaluate(node, context);
			transformedNode.putAll((ObjectNode) exprNode);
		}
	}

	// private Map<String, EvaluableExpression> mappings = new LinkedHashMap<String, EvaluableExpression>();
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

	public void addMapping(String target, EvaluableExpression expression) {
		this.mappings.add(new Mapping(target, expression));
	}

	public List<Mapping> getMappings() {
		return mappings;
	}

	public int getMappingSize() {
		return this.mappings.size();
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.mappings.hashCode();
		return result;
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
	public void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment) {
		for (Mapping mapping : this.mappings)
			if (mapping.getExpression() instanceof ContainerExpression)
				((ContainerExpression) mapping.getExpression()).replace(toReplace, replaceFragment);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		ObjectNode transformedNode = OBJECT_MAPPER.createObjectNode();
		for (Mapping mapping : this.mappings)
			mapping.evaluate(transformedNode, node, context);
		return transformedNode;
	}

	public void addMapping(Mapping mapping) {
		mappings.add(mapping);
	}

	public Mapping getMapping(int index) {
		return mappings.get(index);
	}

}
