package eu.stratosphere.sopremo.base;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.TextNode;

public class GlobalEnumeration extends ElementaryOperator<GlobalEnumeration> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8552367347318407324L;

	public static final EvaluationExpression CONCATENATION = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3340948936846733311L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return TextNode.valueOf(String.format("%d_%d", ((ArrayNode)node).get(0), ((ArrayNode)node).get(1)));
		}
	};

	public static final EvaluationExpression LONG_COMBINATION = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -9084196126957908547L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return LongNode.valueOf((((LongNode)((ArrayNode)node).get(0)).getLongValue() << 48) + ((LongNode)((ArrayNode)node).get(1)).getLongValue());
		}
	};

	private EvaluationExpression enumerationExpression = EvaluationExpression.AS_KEY;

	private EvaluationExpression idGeneration = CONCATENATION;

	public EvaluationExpression getEnumerationExpression() {
		return this.enumerationExpression;
	}

	public String getEnumerationFieldName() {
		if (this.enumerationExpression instanceof ObjectCreation
			&& ((ObjectCreation) this.enumerationExpression).getMappingSize() == 2)
			return (String) ((ObjectCreation) this.enumerationExpression).getMapping(1).getTarget();
		return null;
	}

	public EvaluationExpression getIdGeneration() {
		return this.idGeneration;
	}

	public void setEnumerationExpression(final EvaluationExpression enumerationExpression) {
		if (enumerationExpression == null)
			throw new NullPointerException();

		this.enumerationExpression = enumerationExpression;
	}

	public void setEnumerationFieldName(final String field) {
		if (field == null)
			throw new NullPointerException();

		final ObjectCreation objectMerge = new ObjectCreation();
		objectMerge.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		objectMerge.addMapping(field, new InputSelection(1));
		this.enumerationExpression = objectMerge;
	}

	public void setIdGeneration(final EvaluationExpression idGeneration) {
		if (idGeneration == null)
			throw new NullPointerException("idGeneration must not be null");

		this.idGeneration = idGeneration;
	}

	public static class Implementation extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private EvaluationExpression enumerationExpression, idGeneration;

		private long counter;

		private ArrayNode params;

		@Override
		public void configure(final Configuration parameters) {
			super.configure(parameters);
			final IntNode taskId = new IntNode(parameters.getInteger(AbstractTask.TASK_ID, 0));
			this.counter = 0;
			this.params = JsonUtil.asArray(taskId, LongNode.valueOf(this.counter));
		}

		@Override
		protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
			this.params.set(1, LongNode.valueOf(this.counter++));
			final JsonNode id = this.idGeneration.evaluate(this.params, this.getContext());

			if (this.enumerationExpression == EvaluationExpression.AS_KEY)
				out.collect(id, value);
			else
				out.collect(key, this.enumerationExpression.evaluate(JsonUtil.asArray(value, id), this.getContext()));
		}
	}

}
